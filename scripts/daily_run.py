"""带幂等守卫的每日调度入口。

为什么需要它
------------
GitHub Actions 的 schedule 只是"尽力而为"：高峰期经常延迟 10~40 分钟，偶尔整条跳过。
所以同一个 workflow 里挂了多条 cron（每 20 分钟一条），由本脚本负责去重与补跑：

  1. 今天已经成功推送过  → 立即退出（绝不重复推送到群里）
  2. 今天还没推送过      → 正常执行；任何一步失败就非 0 退出，等下一个 cron 补跑
  3. 数据库为空（缓存失效/首次）→ 自动先做一次全量回填，再继续
  4. 非交易日            → 18:00 后仍无当日数据即判定休市，标记完成不再重试
  5. 周末                → 直接跳过

退出码
------
  0 = 已完成（含"跳过"）    非 0 = 本次失败，需要下一次调度重试
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from sequoia_x.core.config import get_settings  # noqa: E402
from sequoia_x.core.logger import get_logger  # noqa: E402
from sequoia_x.data.engine import DataEngine  # noqa: E402

TZ = ZoneInfo("Asia/Shanghai")
STATE_PATH = ROOT / "data" / "last_success.json"
FORCE = "--force" in sys.argv
# 该时点之后仍拿不到当日数据，就认为是休市（baostock 官方口径：交易日 17:30 左右更新）
HOLIDAY_CUTOFF_HOUR = 18
# 数据源抽风时的容忍度：库内最新交易日距今天超过这个天数，才判定为"真休市/数据过期"不推送。
# 5 天覆盖周末（周一跑时最新是上周五 = 3 天）与 3~4 天的小长假。
# 在此之内，即使今天这一根 K 线没拉到，也照常用最近交易日数据算选股并推送。
MAX_DATA_LAG_DAYS = int(os.environ.get("MAX_DATA_LAG_DAYS", "5"))
# 只保留最近 N 个交易日。滚动窗口让库体积恒定，不会随年份膨胀。
# 400 日 ≈ 1.6 年；最吃历史的 RPS 策略需要 250 日，留足余量。
KEEP_DAYS = int(os.environ.get("KEEP_DAYS", "400"))

log = get_logger("daily_run")


def gh_output(key: str, value: str) -> None:
    """把结果写给 GitHub Actions，供后续步骤判断"要不要保存缓存"。"""
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(f"{key}={value}\n")


def trim_history(db_path: str, keep: int) -> int:
    """滚动裁剪：只保留最近 keep 个交易日，返回删除行数。

    为什么要裁：Actions 缓存按整包替换，库越大每月占用的缓存越多。
    固定窗口能让体积恒定（约 260MB），永远贴着 10GB 免费额度以下。
    """
    with sqlite3.connect(db_path) as conn:
        dates = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT date FROM stock_daily ORDER BY date DESC LIMIT ?",
                (keep,),
            ).fetchall()
        ]
        if len(dates) < keep:
            return 0
        cutoff = dates[-1]
        cur = conn.execute("DELETE FROM stock_daily WHERE date < ?", (cutoff,))
        conn.commit()
        dropped = cur.rowcount

    # 删除只把页面标记为空闲，文件本身不会变小。
    # 大比例裁剪（例如首次把两年历史缩到窗口内）后必须 VACUUM 压实，
    # 否则缓存里躺着的仍是没裁剪前的大文件。
    if dropped > 20000:
        with sqlite3.connect(db_path) as c2:
            c2.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            c2.execute("VACUUM")
    return dropped


def now() -> datetime:
    return datetime.now(TZ)


def today_str() -> str:
    return now().strftime("%Y-%m-%d")


def read_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_state(payload: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def db_row_count(db_path: str) -> int:
    try:
        with sqlite3.connect(db_path) as conn:
            return conn.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
    except sqlite3.Error:
        return 0


def has_date(db_path: str, day: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM stock_daily WHERE date = ? LIMIT 1", (day,)
        ).fetchone()
    return row is not None


def db_latest_date(db_path: str) -> str:
    """库内最新交易日。数据源抽风时用它判断数据是否还够新。"""
    try:
        with sqlite3.connect(db_path) as conn:
            return conn.execute("SELECT MAX(date) FROM stock_daily").fetchone()[0] or ""
    except sqlite3.Error:
        return ""


def days_behind(latest: str, today: str) -> int:
    """最新数据距今天多少自然日。"""
    if not latest:
        return 9999
    from datetime import date

    return (date.fromisoformat(today) - date.fromisoformat(latest)).days


def main() -> int:
    today = today_str()
    stamp = now().strftime("%Y-%m-%d %H:%M:%S")

    if now().weekday() >= 5 and not FORCE:
        log.info(f"{today} 是周末，跳过")
        return 0

    state = read_state()
    if state.get("last_success_date") == today and not FORCE:
        log.info(f"{today} 已于 {state.get('finished_at')} 完成，本次跳过")
        return 0

    settings = get_settings()
    engine = DataEngine(settings)

    # ① 库为空 → 首次回填（Actions 缓存失效、或首次部署）
    rows = db_row_count(settings.db_path)
    if rows == 0:
        # 只回填滚动窗口真正需要的历史。
        # 默认 START_DATE=2024-01-01 会拉 690 多个交易日的量，紧接着又被裁到
        # KEEP_DAYS，等于白跑四成的请求。按自然日折算（400 交易日 ≈ 560 自然日）
        # 再留节假日余量，直接按需拉取。
        warm_start = (now() - timedelta(days=int(KEEP_DAYS * 1.45) + 40)).strftime("%Y-%m-%d")
        log.info(f"本地数据库为空，先执行首次回填（起始 {warm_start}，保留 {KEEP_DAYS} 交易日）……")
        fast = ROOT / "scripts" / "backfill_parallel.py"
        if fast.exists():
            # 并行版：每进程只登录一次，3 并发（实测 8 并发会触发 baostock 黑名单）
            rc = subprocess.call(
                [sys.executable, "-u", str(fast), "--workers", "3", "--start", warm_start],
                cwd=str(ROOT),
            )
        else:
            rc = subprocess.call([sys.executable, "-u", "main.py", "--backfill"], cwd=str(ROOT))
        rows_after = db_row_count(settings.db_path)
        if rows_after == 0:
            log.error(f"回填失败（退出码 {rc}），库仍为空，等待下一次调度重试")
            return 1
        if rc != 0:
            # 部分票失败不该否定整轮：库里已有数据的票就是有效成果，
            # 缺的票下次 build_tasks 会重新生成任务（库里没有它们的记录）。
            log.warning(
                f"回填部分失败（退出码 {rc}），已入库 {rows_after} 条；"
                f"缺失的股票会在今日增量与下次运行中自动补齐"
            )
        # 关键：只要库里有了数据就立刻通知上层保存缓存。
        # 否则后续增量/策略/推送任一步失败，都会把回填成果一起丢掉
        # ——云端实测过一次：196 万行、两小时，因为 403 只票失败而全部作废。
        gh_output("updated", "true")
        log.info(f"回填完成，共 {rows_after} 条")
        # 回填默认从 START_DATE 起，先裁到滚动窗口，避免第一次就缓存一个超大库
        if trim_history(settings.db_path, KEEP_DAYS):
            log.info(f"回填后裁剪到最近 {KEEP_DAYS} 个交易日")

    # ② 增量同步：只补当天 —— best-effort，绝不阻断后续策略。
    # 踩过的坑：baostock 在 Azure 上偶发 "网络接收错误"，探针 login 失败后旧代码
    # 直接 return 1，连策略都不跑，结果库里有 251MB 数据却一条消息都没推出去。
    # 拿不到今天这一根 K 线 ≠ 不能用库里最近一个交易日的数据算选股。
    log.info("增量同步当日数据……")
    fast = ROOT / "scripts" / "backfill_parallel.py"
    if fast.exists():
        rc = subprocess.call(
            [sys.executable, "-u", str(fast), "--workers", "3", "--today-only"],
            cwd=str(ROOT),
        )
        if rc != 0:
            log.warning(
                f"增量同步失败（退出码 {rc}），改用库内最近交易日数据继续跑策略，不阻断推送"
            )
    else:
        try:
            engine.sync_today_bulk()
        except Exception as exc:  # noqa: BLE001
            log.warning(f"预同步异常：{exc}，改用库内已有数据继续")

    # ②' 港股通同步：新浪名单 + 腾讯日K（独立数据源，与 baostock 无关）。
    # 失败只影响港股通卡片，绝不阻断 A 股主流程。
    hk_sync = ROOT / "scripts" / "hk_sync.py"
    if hk_sync.exists():
        hk_rc = subprocess.call(
            [sys.executable, "-u", str(hk_sync), "--today-only"],
            cwd=str(ROOT),
        )
        if hk_rc != 0:
            log.warning(f"港股通同步失败（退出码 {hk_rc}），A 股主流程继续")
    else:
        log.info("未找到 scripts/hk_sync.py，跳过港股通同步")

    latest = db_latest_date(settings.db_path)
    lag = days_behind(latest, today)
    log.info(f"库内最新交易日 {latest or '无'}，距今天 {lag} 个自然日")

    # 只有"真休市 / 长假 / 数据严重过期"才跳过，不能用"库里没有今天的数据"来判定
    # —— 后者也可能是数据源临时故障，直接跳过会漏推一整天。
    if lag > MAX_DATA_LAG_DAYS:
        log.info(
            f"数据滞后 {lag} 天（>{MAX_DATA_LAG_DAYS}），判定为休市或数据过期，本次不推送"
        )
        write_state(
            {
                "last_success_date": today,
                "finished_at": stamp,
                "status": "non_trading_day",
            }
        )
        gh_output("updated", "true")
        return 0

    if not has_date(settings.db_path, today):
        log.warning(
            f"今日 {today} 数据未到位，仍基于最近交易日 {latest} 推送选股结果"
            f"（滞后 {lag} 天，数据源恢复后会自动补齐）"
        )

    # ③ 正式跑：策略 + 推送
    log.info("开始执行 main.py（策略计算 + 飞书推送）……")
    rc = subprocess.call([sys.executable, "-u", "main.py", "--skip-sync"], cwd=str(ROOT))
    if rc != 0:
        log.error(f"main.py 退出码 {rc}，本次视为失败，等待下一次调度补跑")
        return rc if rc > 0 else 1

    # ④ 滚动裁剪：只留最近 KEEP_DAYS 个交易日，让库体积恒定
    dropped = trim_history(settings.db_path, KEEP_DAYS)
    if dropped:
        log.info(f"滚动裁剪：删除 {dropped} 条早于最近 {KEEP_DAYS} 个交易日的数据")

    write_state(
        {
            "last_success_date": today,
            "finished_at": now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "ok",
        }
    )
    # 通知 workflow：数据有变化，才需要保存缓存。空跑的 cron 不保存，省额度。
    gh_output("updated", "true")
    log.info("今日任务完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
