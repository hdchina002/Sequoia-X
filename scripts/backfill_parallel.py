"""并行回填脚本（替代 `python main.py --backfill`）。

为什么要自己写一个
------------------
原版回填是单线程串行，且 `main.py` 里设了 `socket.setdefaulttimeout(10.0)`：
一旦 baostock 某个查询卡住，10 秒超时 → 代码再重试 3 次（间隔 2/4/8 秒），
单只票就要烧掉 30 秒左右。全市场 5200 只票 → 十几个小时，实际不可用。

本脚本的做法：
  * 多进程并行（每进程独立 login），默认 6 并发
  * 单票失败重试 3 次，超时 20 秒，失败就跳过并记录，不阻塞整体
  * 父进程统一入库，`INSERT OR REPLACE` 天然幂等，中断可重跑续传
  * 已在库里的股票（末日 >= 全库最新日期）自动跳过

用法：
  .venv/Scripts/python.exe scripts/backfill_parallel.py                # 默认 6 进程
  .venv/Scripts/python.exe scripts/backfill_parallel.py --workers 8
  .venv/Scripts/python.exe scripts/backfill_parallel.py --start 2025-01-01
"""

from __future__ import annotations

import argparse
import os
import socket
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from multiprocessing import Pool
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

DB_PATH = os.environ.get("DB_PATH", "data/sequoia_v2.db")
START_DATE = os.environ.get("START_DATE", "2024-01-01")
FIELDS = ["date", "open", "high", "low", "close", "volume", "amount"]
SOCKET_TIMEOUT = 20
MAX_RETRIES = 3

_INSERT_SQL = (
    "INSERT OR REPLACE INTO stock_daily "
    "(symbol, date, open, high, low, close, volume, turnover) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)


def _to_bs_code(symbol: str) -> str:
    prefix = "sh" if symbol.startswith(("6", "9")) else "sz"
    return f"{prefix}.{symbol}"


_BS_READY = False


def _ensure_login() -> None:
    """每个子进程只登录一次，避免每只票重复 login/logout（实测占 80% 耗时）。"""
    global _BS_READY
    if _BS_READY:
        return
    import baostock as bs

    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"login失败:{lg.error_msg}")
    _BS_READY = True


def fetch_one(task: tuple[str, str, str]) -> tuple[str, list[tuple] | None, str]:
    """子进程：拉取单只股票的区间数据。返回 (symbol, rows|None, err)。"""
    symbol, start, end = task
    socket.setdefaulttimeout(SOCKET_TIMEOUT)

    import baostock as bs

    try:
        _ensure_login()
    except Exception as exc:  # noqa: BLE001
        return symbol, None, f"{type(exc).__name__}:{exc}"

    for attempt in range(MAX_RETRIES):
        try:
            rs = bs.query_history_k_data_plus(
                _to_bs_code(symbol),
                ",".join(FIELDS),
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="1",  # 后复权
            )
            if rs.error_code != "0":
                raise RuntimeError(rs.error_msg)

            rows: list[tuple] = []
            while rs.next():
                r = rs.get_row_data()
                # baostock 对停牌日返回空字符串，必须过滤：
                # 直接写进 REAL 列会被 SQLite 存成 TEXT，读回来整列变 object，
                # 后续 rolling().mean() 会抛 "No numeric types to aggregate"。
                try:
                    o, h, lo, c, v, amt = (float(x) for x in r[1:7])
                except (TypeError, ValueError):
                    continue
                if c <= 0 or v <= 0:
                    continue
                # (symbol, date, open, high, low, close, volume, turnover)
                rows.append((symbol, r[0], o, h, lo, c, v, amt))
            return symbol, rows, ""
        except Exception as exc:  # noqa: BLE001
            msg = f"{type(exc).__name__}:{exc}"
            # 黑名单是 IP 级封禁，重试只会加深惩罚，直接返回
            if "黑名单" in msg or "10001011" in msg:
                return symbol, None, "BLACKLIST"
            if attempt == MAX_RETRIES - 1:
                return symbol, None, msg
            # 注意：失败后**不重新登录**，只在同一会话内退避重试。
            # 反复 login/logout 正是触发 baostock 黑名单的原因（实测踩过）。
            time.sleep(2.0 * (attempt + 1))

    return symbol, None, "unknown"


def get_all_symbols() -> list[str]:
    import baostock as bs

    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {lg.error_msg}")
    try:
        rs = bs.query_stock_basic(code_name="", code="")
        out = []
        while rs.next():
            row = rs.get_row_data()
            code, status, stock_type = row[0], row[4], row[5]
            if status == "1" and stock_type == "1":
                out.append(code.split(".")[1])
        return out
    finally:
        bs.logout()


def build_tasks(symbols: list[str]) -> tuple[list[tuple[str, str, str]], str]:
    """按库里已有进度生成任务；返回 (tasks, 全库最新日期)。"""
    today = date.today().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS stock_daily (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   symbol TEXT NOT NULL, date TEXT NOT NULL,
                   open REAL, high REAL, low REAL, close REAL,
                   volume REAL, turnover REAL,
                   UNIQUE (symbol, date))"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date)"
        )
        global_max = conn.execute("SELECT MAX(date) FROM stock_daily").fetchone()[0]
        have = dict(
            conn.execute("SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol").fetchall()
        )
        conn.commit()

    tasks = []
    for s in symbols:
        last = have.get(s)
        if last and global_max and last >= global_max:
            continue
        start = START_DATE
        if last:
            start = (date.fromisoformat(last) + timedelta(days=1)).strftime("%Y-%m-%d")
        if start > today:
            continue
        tasks.append((s, start, today))
    return tasks, global_max or ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--start", default=None, help="覆盖 START_DATE")
    args = ap.parse_args()

    global START_DATE
    if args.start:
        START_DATE = args.start

    t0 = datetime.now()
    print(f"[{t0:%H:%M:%S}] 获取全市场股票列表……", flush=True)
    symbols = get_all_symbols()
    print(f"[{datetime.now():%H:%M:%S}] 共 {len(symbols)} 只股票", flush=True)

    tasks, global_max = build_tasks(symbols)
    print(
        f"[{datetime.now():%H:%M:%S}] 库内最新日期={global_max or '空'}，"
        f"待更新 {len(tasks)} 只，起始={START_DATE}，并发={args.workers}",
        flush=True,
    )
    if not tasks:
        print("全部已是最新，无需回填。", flush=True)
        return 0

    ok = fail = rows_written = 0
    blacklist_hits = 0
    failed: list[str] = []
    total = len(tasks)
    last_report = time.time()
    t_fetch = time.time()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    with Pool(args.workers) as pool:
        for i, (symbol, rows, err) in enumerate(
            pool.imap_unordered(fetch_one, tasks, chunksize=1), 1
        ):
            if rows is None:
                fail += 1
                failed.append(f"{symbol}({err})")
                with open(ROOT / "logs" / "failures.log", "a", encoding="utf-8") as fh:
                    fh.write(f"{symbol}\t{err}\n")
                if err == "BLACKLIST":
                    blacklist_hits += 1
            else:
                blacklist_hits = 0
                if rows:
                    conn.executemany(_INSERT_SQL, rows)
                    conn.commit()
                    rows_written += len(rows)
                ok += 1

            # 熔断：连续 20 次都是黑名单，说明本机 IP 被封，继续跑没有意义
            if blacklist_hits >= 20:
                conn.close()
                print(
                    "\n[熔断] 连续 20 次返回 BLACKLIST，本机 IP 已被 baostock 封禁。\n"
                    "      已入库数据不会丢失，解封后重跑本脚本会自动续传。\n"
                    "      建议：把回填交给 GitHub Actions（Azure IP 不受影响）。",
                    flush=True,
                )
                return 4

            if time.time() - last_report > 5 or i == total:
                speed = i / max(time.time() - t_fetch, 0.1)
                eta = (total - i) / max(speed, 0.01)
                print(
                    f"[{datetime.now():%H:%M:%S}] {i}/{total} "
                    f"({i/total*100:.1f}%) 成功{ok} 失败{fail} "
                    f"写入{rows_written}行 预计剩余{eta/60:.1f}分钟",
                    flush=True,
                )
                last_report = time.time()

    conn.close()

    # 收尾：把 WAL 落盘并切回 DELETE 模式，再压实文件。
    # 目的：让整个数据库只存在于单个 .db 文件里，方便 GitHub Actions 缓存搬运。
    try:
        with sqlite3.connect(DB_PATH) as c2:
            c2.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            c2.execute("PRAGMA journal_mode=DELETE")
            c2.execute("VACUUM")
        print(f"[{datetime.now():%H:%M:%S}] 数据库已压实", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"压实数据库失败（不影响数据）：{exc}", flush=True)

    cost = (datetime.now() - t0).total_seconds()
    print(
        f"\n回填结束：成功 {ok} / 失败 {fail} / 写入 {rows_written} 行 / 耗时 {cost/60:.1f} 分钟",
        flush=True,
    )
    if failed:
        print(f"失败清单（前 20）：{failed[:20]}", flush=True)
        print("可重跑本脚本自动续传。", flush=True)
    return 0 if fail == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
