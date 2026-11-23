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
from datetime import datetime
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

    # ① 库为空 → 全量回填（Actions 缓存失效、或首次部署）
    rows = db_row_count(settings.db_path)
    if rows == 0:
        log.info("本地数据库为空，先执行全量回填……")
        fast = ROOT / "scripts" / "backfill_parallel.py"
        if fast.exists():
            # 并行版：每进程只登录一次，比原版快一个数量级
            rc = subprocess.call([sys.executable, "-u", str(fast), "--workers", "3"], cwd=str(ROOT))
        else:
            rc = subprocess.call([sys.executable, "-u", "main.py", "--backfill"], cwd=str(ROOT))
        if rc != 0 or db_row_count(settings.db_path) == 0:
            log.error(f"回填失败（退出码 {rc}），等待下一次调度重试")
            return 1
        log.info(f"回填完成，共 {db_row_count(settings.db_path)} 条")
        # 回填默认从 START_DATE 起，先裁到滚动窗口，避免第一次就缓存一个超大库
        if trim_history(settings.db_path, KEEP_DAYS):
            log.info(f"回填后裁剪到最近 {KEEP_DAYS} 个交易日")

    # ② 预同步：先确认今天到底是不是交易日
    # 先探一次登录：若本机 IP 已被 baostock 拉黑，直接退出，
    # 避免 sync_today_bulk 里 8 个进程同时登录、把封禁坐实。
    import baostock as _bs

    _probe = _bs.login()
    if _probe.error_code != "0":
        _msg = _probe.error_msg or ""
        if "黑名单" in _msg or _probe.error_code == "10001011":
            log.error(f"本机 IP 已被 baostock 列入黑名单（{_msg}），本次跳过。"
                      f"已入库数据不受影响；建议把回填/日常运行交给 GitHub Actions。")
            return 1
        log.warning(f"baostock 登录异常：{_probe.error_code} {_msg}")
    else:
        try:
            _bs.logout()
        except Exception:  # noqa: BLE001
            pass

    log.info("预同步当日数据……")
    try:
        engine.sync_today_bulk()
    except Exception as exc:  # noqa: BLE001
        log.error(f"预同步失败：{exc}")
        return 1

    if not has_date(settings.db_path, today):
        if now().hour >= HOLIDAY_CUTOFF_HOUR:
            log.info(f"{HOLIDAY_CUTOFF_HOUR}:00 后仍无 {today} 数据，判定为休市，标记完成")
            write_state(
                {
                    "last_success_date": today,
                    "finished_at": stamp,
                    "status": "non_trading_day",
                }
            )
            gh_output("updated", "true")
        else:
            log.info(f"{today} 暂无数据（可能尚未发布），等待下一次调度")
        return 0

    # ③ 正式跑：策略 + 推送
    log.info("开始执行 main.py（策略计算 + 飞书推送）……")
    rc = subprocess.call([sys.executable, "-u", "main.py"], cwd=str(ROOT))
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
