# bsc-aws-exporter

> 把 BSC 链上数据按天导成 Parquet，结构对齐 AWS Public Blockchain Datasets。

`Python 3.10+`  ·  `ethereum-etl`  ·  `pyarrow`  ·  `boto3`

---

输出目录结构与 AWS Public Blockchain Datasets 的 ETH 数据集保持一致：

```
s3://{bucket}/{prefix}/blocks/date=YYYY-MM-DD/blocks.parquet
s3://{bucket}/{prefix}/transactions/date=YYYY-MM-DD/transactions.parquet
```

底层导出由 [`ethereum-etl`](https://github.com/blockchain-etl/ethereum-etl) 完成，
本项目负责：按日期定位区块范围、CSV → Parquet（schema 对齐 + 时间戳精确切边）、
上传 S3、断点续跑、systemd 定时调度。

## 项目结构

```
bsc-aws-exporter/
├── exporter.py               # 主程序
├── config.yaml.example       # 配置模板
├── requirements.txt          # Python 依赖
├── README.md
├── .gitignore
└── systemd/                  # 定时部署套件
    ├── bsc-exporter.service
    ├── bsc-exporter.timer
    └── install.sh
```

---

## 依赖

```
ethereum-etl>=2.4.0
pyarrow>=14.0.0
boto3>=1.34.0
pyyaml>=6.0
web3>=5.31,<6.0      # ethereum-etl 2.x 依赖 web3 v5 API
setuptools<70        # ethereum-etl 2.x 使用了 setuptools>=70 移除的 pkg_resources
```

安装：

```bash
pip install -r requirements.txt
```

建议用 Python 3.10+ 的 virtualenv 隔离。

---

## 配置

复制 `config.yaml.example` 为 `config.yaml` 并按需修改：

```yaml
rpc_url: "https://bsc-dataseed.bnbchain.org"   # 建议用自己的 full node 做回填

s3:
  bucket: "aws-public-blockchain"
  prefix: "v1.1/bnb"
  region: "us-east-2"

export:
  ethereumetl_batch_size: 100   # 每次 RPC batch call 的 block 数
  ethereumetl_workers: 5        # ethereum-etl 并发 worker
  row_group_size: 50000         # Parquet row group 大小
  work_dir: "/tmp/bsc-export"   # 本地临时目录 + 进度文件
```

AWS 凭证走 boto3 默认链：环境变量 / `~/.aws/credentials` / IAM Role 都可以。

---

## 使用

```bash
# 导出昨天的数据（cron 定时任务推荐用法）
python exporter.py --config config.yaml

# 导出指定单日
python exporter.py --config config.yaml --date 2024-01-15

# 区间回填（含端点），4 进程并行
python exporter.py --config config.yaml \
    --start 2020-08-29 --end 2026-04-29 -j 4

# 本地 dry-run，只生成 Parquet 不上传
python exporter.py --config config.yaml --date 2024-01-15 --dry-run
```

参数：

| 参数 | 说明 |
|------|------|
| `--config` | 配置文件路径（必填） |
| `--date` | 导出单个日期 `YYYY-MM-DD` |
| `--start` / `--end` | 区间回填，必须成对出现 |
| `-j`, `--parallel` | 回填的并行进程数（默认 1） |
| `--dry-run` | 只在本地生成 Parquet，跳过 S3 上传 |

不传 `--date` 也不传区间时，默认导出 UTC 昨天。

---

## 断点续跑

`work_dir/progress.txt` 记录已完成的日期，重跑时会自动跳过。
S3 上若已存在两个目标 Parquet 文件也会跳过。

被跳过的判定顺序：

1. `progress.txt` 命中 → 跳过
2. S3 上 blocks.parquet + transactions.parquet 都存在 → 跳过并补登记 progress
3. 否则正常导出

并行模式下用 `flock` 保护 `progress.txt` 写入。

---

## 工作流程

```
RPC bisect 找出当天的 start_block；end_block 用估算+margin 略微多取
        ↓
ethereumetl export_blocks_and_transactions  →  blocks.csv / transactions.csv
        ↓
pyarrow CSV → Parquet：按 timestamp / block_timestamp 过滤到精确日期边界
        ↓
boto3 multipart upload 到 S3，head_object 校验 ContentLength
        ↓
progress.txt 标记完成，清理临时目录
```

边界处理用 timestamp 过滤而非二分：start_block 必须精确（没下载的块没法过滤），
end_block 可以宽松——估算 + ESTIMATE_MARGIN 后多取几千个块，
最后由 pyarrow 按 `timestamp` / `block_timestamp` 字段过滤到 `[day_start_ts, day_end_ts)`，
天然消除"下一天第一个块"这种 off-by-one 歧义。

时间戳→区块的估算函数感知 BSC 主网硬分叉对区块间隔的调整：

| Fork | UTC 时间 | 区块间隔 |
|------|---------|---------|
| Genesis | 2020-08-29 03:24:28 | 3000ms |
| Lorentz | 2025-04-29 05:05:00 | 1500ms |
| Maxwell | 2025-06-30 02:30:00 | 750ms |
| Fermi   | 2026-01-14 02:30:00 | 450ms |

如未来再有硬分叉缩短区块间隔，需要在 `exporter.py` 顶部的 `BSC_FORKS` 列表中追加一项。
即使忘了更新，二分搜索也会兜底（性能损失，但结果仍正确）。

S3 上传顺序为先 `transactions.parquet` 再 `blocks.parquet`，
后者作为完成 marker：`_date_on_s3` 要求两者都存在才视为已完成，
中途崩溃不会让下游消费方读到半成品。

---

## 部署：systemd timer（推荐）

`systemd/` 目录提供了完整的部署套件：

```
systemd/
├── bsc-exporter.service   # 一次性单元，跑一次就退出
├── bsc-exporter.timer     # 每天 UTC 01:00 触发，错过会补跑
└── install.sh             # 创建用户、装依赖、装 unit 文件
```

### 安装

```bash
sudo ./systemd/install.sh
sudo vi /etc/bsc-exporter/config.yaml         # 改成自己的 rpc_url / S3 bucket
sudo systemctl start bsc-exporter.service     # 先手动跑一次验证
sudo journalctl -u bsc-exporter -f            # 看日志
sudo systemctl enable --now bsc-exporter.timer  # 启用每日定时
```

### 关键设计

- **`Type=oneshot`**：跑完就退出，不是常驻进程。崩溃恢复 / 升级部署都靠下次定时
- **`Persistent=true`**：机器停机错过的执行会在重启后补跑
- **`RandomizedDelaySec=600`**：随机延迟 0–10 分钟，避免多节点同时打 RPC
- **`ConcurrencyPolicy` 等价物**：systemd `oneshot` 单元天然不会并发触发自己
- **沙箱**：`ProtectSystem=strict` + `PrivateTmp=true` 等硬化选项已开启

### 路径约定

| 路径 | 用途 |
|------|------|
| `/opt/bsc-exporter/` | 代码 + venv |
| `/etc/bsc-exporter/config.yaml` | 配置 |
| `/var/lib/bsc-exporter/` | progress.txt + 临时 CSV/Parquet（对应 `export.work_dir`） |
| `/var/log/bsc-exporter/` | 预留，目前日志走 journald |

`install.sh` 顶部的环境变量可覆盖默认路径。

### 查看 / 排错

```bash
systemctl list-timers bsc-exporter.timer    # 下次触发时间
journalctl -u bsc-exporter --since "2 days ago"
journalctl -u bsc-exporter -p err           # 只看错误
systemctl status bsc-exporter.service       # 上次运行结果 + 退出码
```

---

## 其他运维事项

- **回填**：定时任务之外的一次性大批量补数据，直接登录机器手动跑，不要走 timer：
  ```bash
  sudo -u bsc /opt/bsc-exporter/venv/bin/python /opt/bsc-exporter/exporter.py \
      --config /etc/bsc-exporter/config.yaml \
      --start 2020-08-29 --end 2026-04-29 -j 4
  ```
- **失败排查**：日志带日期前缀 `[YYYY-MM-DD]`；失败时临时目录会被清理，
  `--dry-run` 模式下文件保留在 `work_dir/{date}/` 便于人工检查。
- **磁盘**：`work_dir` 单日峰值几个 GB（CSV + Parquet 同时存在），
  正常完成自动清理；并行 `-j N` 时同时占用 N 倍。
- **AWS 凭证**：EC2 上推荐用 instance profile（IAM Role），service 文件里
  无需配置；其他环境把 key 写到 `/etc/bsc-exporter/aws.env` 并取消
  `EnvironmentFile=` 那行的注释，文件权限 `chmod 600`。
