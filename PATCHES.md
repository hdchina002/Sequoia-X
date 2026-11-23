# 本地修改记录

上游：`sngyai/Sequoia-X` master（MIT）。为适配本地 Windows 环境与 GitHub Actions 调度，做了以下改动。
**升级上游代码时，这几处需要重新应用。**

## 1. `sequoia_x/data/engine.py` — 修复数据缺口

原实现（`sync_today_bulk`）：

```python
for d in df["date"].unique().tolist():
    conn.execute("DELETE FROM stock_daily WHERE date = ?", (d,))
```

先按日期把**当天全市场**记录删掉再重插。若某只票当日未返回数据，它已有的记录会被连带删除，
长期运行会在库里留下缺口，影响均线/形态计算。

改为只删除本次实际取到的 `(symbol, date)` 组合：

```python
pairs = sorted({(str(s), str(d)) for s, d in zip(df["symbol"], df["date"])})
conn.executemany("DELETE FROM stock_daily WHERE symbol = ? AND date = ?", pairs)
```

## 2. `sequoia_x/notify/feishu.py` — 推送结果可判定

`FeishuNotifier.send()` 由 `-> None` 改为 `-> bool`：成功返回 `True`，HTTP 异常或飞书返回
`code != 0` 返回 `False`。原版只写日志，调用方无法知道推没推出去。

## 3. `main.py` — 推送失败时以非 0 退出

统计 `push_failed`，只要有一个策略推送失败就 `sys.exit(2)`。
这样 GitHub Actions 能识别"本次没成功"，由下一条 cron 自动补跑。

## 4. 新增 `scripts/backfill_parallel.py` — 并行回填

原版 `main.py --backfill` 是单线程，且 `main.py` 设了 `socket.setdefaulttimeout(10.0)`：
baostock 一旦卡住，10 秒超时 + 3 次重试（间隔 2/4/8 秒），单票要烧 30 秒，
全市场跑完要十几个小时（实测 6 分钟只完成 29 只）。

新版：多进程并行（默认 3）、每进程只 login 一次、`INSERT OR REPLACE` 幂等可续传、
结束自动 checkpoint + VACUUM 压实成单文件。

> ⚠️ **并发数不要调高。** 实测 8 并发 + 失败后反复 logout/login，会触发 baostock 的
> 防滥用机制，直接返回 `10001011 黑名单用户`，把整台机器的 IP 封掉（几十分钟到几小时）。
> 现版本默认 3 并发、失败只在同一会话内退避重试、绝不再重新登录；连续 20 次黑名单自动熔断。
> 另外 `main.py` 平时设的 `socket.setdefaulttimeout(10.0)` 也在这里被覆盖成 20 秒。

## 5. 新增 `scripts/daily_run.py` — 幂等调度器

GitHub Actions 的 cron 不保证准点（延迟 10~40 分钟甚至整条跳过），所以在 workflow 里
挂了 8 条 cron（每 20 分钟一条），由本脚本去重：

- 今天已成功 → 立刻退出，不重复推送
- 今天没成功 → 正常跑；失败非 0 退出，等下一轮补
- 库为空（缓存失效）→ 先自动回填
- 18:00 后仍无当日数据 → 判定休市，标记完成后不再重试
- 周末直接跳过

## 6. 新增 `scripts/preview.py` — 本地预览

跑完 7 个策略但不推送，终端打印结果。装好后先用它验证策略是否正常出票。

## 7. 新增 `.github/workflows/sequoia-daily.yml`

8 条 cron（北京时间 17:40 → 20:00）、缓存 `data/` 目录、Webhook 走 Secrets。

**缓存拆成 restore / save 两步**（`actions/cache/restore@v4` + `actions/cache/save@v4`）：
一天 8 次 cron 里只有真正更新了数据的那一次才 save（由 `daily_run.py` 往 `GITHUB_OUTPUT`
写 `updated=true` 控制）。若沿用单个 `actions/cache@v4`，8 次都会存一份整库，
一周就把 10GB 免费额度撑爆。

## 8. `scripts/daily_run.py` — 滚动窗口裁剪

`KEEP_DAYS`（默认 400 个交易日）控制保留窗口，每次成功推送后删除更早的数据。

- 目的：让数据库体积**恒定**，不随年份线性膨胀。400 日 × 5224 只 ≈ 210 万行 ≈ 256MB，
  最吃历史的 RPS 策略只需 250 日，留 150 日余量。
- 大比例裁剪（>2 万行）后自动 `VACUUM` 压实——DELETE 只把页面标记为空闲，
  文件本身不会缩小，不压实的话缓存里仍是裁剪前的大文件。
- 实测：698 个交易日 → 400 日，删 274,961 行，78.9MB → 45.3MB（929 只股票样本）。

## 9. 云端调度三处致命缺陷（2026-11-23 修，上线前发现）

这三处只在 GitHub Actions 上才会炸，本地完全看不出来，注释一下避免以后改回去：

**① `build_tasks` 用全库最新日期判定跳过 → 数据永久停更**

```python
# 错误：所有票互相看齐
if last and global_max and last >= global_max:  # 全库都是 11-20 时，每只票都满足
    continue
```

全库都停在同一天时，每只票都认为"我已经是最新的"，第二天起来一条数据都不会更新。
改为用 `today` 判定（`last >= today` 才跳过）。这类 bug 首次回填时完全正常，
只有跑第二天才暴露——不加对照测试很容易漏（实测：929 只票旧逻辑生成 0 个任务）。

**② `logs/` 目录未入库 → 首次回填崩溃**

`backfill_parallel.py` 会往 `logs/failures.log` 追加失败记录。`logs/` 在 `.gitignore` 里、
不进版本控制，Actions 检出的是干净仓库 → `FileNotFoundError`，回填直接中断。
已在 `main()` 开头 `(ROOT / "logs").mkdir(parents=True, exist_ok=True)`。

**③ 同步做了两遍 → 双倍请求、8 进程重复 login**

`daily_run.py` 自己调 `engine.sync_today_bulk()`，之后 `main.py` 又调一次。
而 `sync_today_bulk` 内部起 **8 个进程、每个进程各自 login**，正是触发黑名单的配方。
现已改为：调度器用 `backfill_parallel.py --today-only`（3 进程、每进程一次登录）同步，
再以 `main.py --skip-sync` 跳过重复同步。

**附带两处**：`--today-only` 先用沪深300指数当探针，休市/数据未发布时立刻退出
（否则要白跑 5224 次查询才发现没数据）；workflow `timeout` 提到 240 分钟并设
`TZ=Asia/Shanghai`（runner 默认 UTC，跨零点会算错日期）。

## 体积与额度实测（2026-11-23）

| 口径 | 数值 |
|---|---|
| 单行开销（含索引） | 128 字节 |
| 2024-01-01 起（694 交易日，全市场） | 约 443 MB |
| 滚动 400 交易日（全市场） | 约 256 MB |
| GitHub Free 缓存额度 | 10 GB / 仓库，超出按 LRU 淘汰 |
| 每天实际写入次数 | 1 次（其余 cron 空跑不存） |
| 月均缓存占用 | 约 5.6 GB（22 个交易日 × 256MB） |

