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
# 进程内已重连次数。上限存在的意义：重连本身是被 baostock 惩罚的行为，
# 无节制重连会把"临时断线"升级成"IP 进黑名单"（本地实测踩过）。
_RECONNECTS = 0
# 每个子进程最多重连几次。黑名单的触发条件是"高频反复 login"（当初是每票一次、5000 次），
# 这里单个进程跑一轮也就断几次，5 次上限折合每小时几十次，远低于危险水平。
_RECONNECT_LIMIT = 5

# 判定"连接已坏"的特征串。命中这些说明会话断了、必须重连才有救；
# 命中"黑名单"则相反——重连只会加深惩罚。
_CONN_ERRORS = (
    "网络接收", "Broken pipe", "ConnectionReset", "Connection reset",
    "ConnectionAborted", "timed out", "Timeout", "RemoteDisconnected",
    "接收数据异常",
)


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


def _reconnect() -> bool:
    """连接坏了就重建一次。受进程内次数上限约束，避免反复登录被封。"""
    global _BS_READY, _RECONNECTS
    if _RECONNECTS >= _RECONNECT_LIMIT:
        return False
    import baostock as bs

    _RECONNECTS += 1
    _BS_READY = False
    try:
        bs.logout()
    except Exception:  # noqa: BLE001
        pass
    time.sleep(3.0)
    try:
        _ensure_login()
        return True
    except Exception:  # noqa: BLE001
        return False


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
            # 断线类错误：会话已经废了，单纯重试必然继续失败
            # （云端实测：某进程断一次后，后面整片票都是"网络接收错误"）。
            # 这里重建连接再试，重连次数受 _RECONNECT_LIMIT 约束。
            if any(k in msg for k in _CONN_ERRORS) and _reconnect():
                continue
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
        # 跳过条件必须用 today 判定。
        # 早期版本写成 last >= global_max（全库最新日期），后果是所有票互相看齐：
        # 全库都停在 11-20 时，每只票都满足 last >= global_max → 全部跳过，
        # 数据永远不再更新。这类 bug 在首次回填时完全看不出来，只有跑第二天才暴露。
        if last and last >= today:
            continue
        start = START_DATE
        if last:
            start = (date.fromisoformat(last) + timedelta(days=1)).strftime("%Y-%m-%d")
        if start > today:
            continue
        tasks.append((s, start, today))
    return tasks, global_max or ""


def today_has_data() -> bool:
    """用沪深300指数当探针，判断今天是否已有行情数据。

    为什么需要：非交易日（周末/长假）与"17:30 前数据未发布"这两种情况，
    如果直接对 5000+ 只票逐一下查询，要白跑十几分钟才得出结论。
    先用一只指数探一下，没有就立刻退出。
    """
    import baostock as bs

    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"login失败:{lg.error_msg}")

    today = date.today().strftime("%Y-%m-%d")
    try:
        rs = bs.query_history_k_data_plus(
            "sh.000001", "date,close",
            start_date=today, end_date=today, frequency="d", adjustflag="3",
        )
        if rs.error_code != "0":
            return False
        while rs.next():
            if rs.get_row_data()[0] == today:
                return True
        return False
    finally:
        try:
            bs.logout()
        except Exception:  # noqa: BLE001
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--start", default=None, help="覆盖 START_DATE")
    ap.add_argument("--today-only", action="store_true",
                    help="仅增量补当天；先用指数探针确认今天有数据，无则立即退出")
    args = ap.parse_args()

    global START_DATE
    if args.start:
        START_DATE = args.start

    # GitHub Actions 检出的是干净仓库，logs/ 不在版本控制里，必须自己建，
    # 否则写 failures.log 时 FileNotFoundError，首次回填直接崩。
    (ROOT / "logs").mkdir(parents=True, exist_ok=True)

    t0 = datetime.now()

    if args.today_only:
        try:
            if not today_has_data():
                print(f"[{t0:%H:%M:%S}] 今日无新数据（休市或尚未发布），跳过", flush=True)
                return 0
        except Exception as exc:  # noqa: BLE001
            print(f"[{t0:%H:%M:%S}] 日期探针失败：{exc}", flush=True)
            return 1
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

    ok = rows_written = 0
    failed_map: dict[str, str] = {}
    pending: list[tuple[str, str, str]] = tasks
    broken = False
    t_fetch = time.time()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # 两轮：第 1 轮跑全量，第 2 轮只补失败的。
    # 云端实测 403 只失败全部集中在"某进程断线后整片连挂"，
    # 重连修好后仍会残留少量超时票，补一轮只花几分钟，能把覆盖率拉回 99%+。
    for round_no in (1, 2):
        if not pending or broken:
            break
        total = len(pending)
        task_by_symbol = {t[0]: t for t in pending}
        retry_tasks: list[tuple[str, str, str]] = []
        blacklist_hits = 0
        round_ok = round_fail = round_rows = 0
        last_report = time.time()
        t_round = time.time()

        print(
            f"[{datetime.now():%H:%M:%S}] 第 {round_no} 轮开始，共 {total} 只",
            flush=True,
        )

        with Pool(args.workers) as pool:
            for i, (symbol, rows, err) in enumerate(
                pool.imap_unordered(fetch_one, pending, chunksize=1), 1
            ):
                if rows is None:
                    round_fail += 1
                    failed_map[symbol] = str(err)
                    retry_tasks.append(task_by_symbol[symbol])
                    with open(ROOT / "logs" / "failures.log", "a", encoding="utf-8") as fh:
                        fh.write(f"{symbol}\t{err}\n")
                    if err == "BLACKLIST":
                        blacklist_hits += 1
                else:
                    blacklist_hits = 0
                    failed_map.pop(symbol, None)
                    if rows:
                        conn.executemany(_INSERT_SQL, rows)
                        conn.commit()
                        round_rows += len(rows)
                    round_ok += 1

                # 熔断：连续 20 次都是黑名单，说明 IP 被封，继续跑只会加深惩罚。
                # 注意这里不 return，先把已入库的数据压实保存，别让几十分钟白费。
                if blacklist_hits >= 20:
                    broken = True
                    print(
                        "\n[熔断] 连续 20 次返回 BLACKLIST，IP 已被 baostock 封禁。\n"
                        "      将保存已入库数据后退出；解封后重跑会自动续传。",
                        flush=True,
                    )
                    pool.terminate()
                    break

                if time.time() - last_report > 10 or i == total:
                    rate = i / max(time.time() - t_round, 0.1)
                    eta = (total - i) / max(rate, 0.01)
                    print(
                        f"[{datetime.now():%H:%M:%S}] 第{round_no}轮 {i}/{total} "
                        f"({i/total*100:.1f}%) 成功{round_ok} 失败{round_fail} "
                        f"写入{round_rows}行 预计剩余{eta/60:.1f}分钟",
                        flush=True,
                    )
                    last_report = time.time()

        ok += round_ok
        rows_written += round_rows
        print(
            f"[{datetime.now():%H:%M:%S}] 第 {round_no} 轮结束："
            f"成功 {round_ok} 失败 {round_fail} 写入 {round_rows} 行",
            flush=True,
        )

        if broken or not retry_tasks:
            break
        print(
            f"[{datetime.now():%H:%M:%S}] 补漏轮：重试 {len(retry_tasks)} 只失败的股票",
            flush=True,
        )
        pending = retry_tasks
        time.sleep(5)

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
        f"\n回填结束：成功 {ok} / 仍失败 {len(failed_map)} / "
        f"写入 {rows_written} 行 / 耗时 {cost/60:.1f} 分钟",
        flush=True,
    )
    if failed_map:
        print(f"失败清单（前 20）：{list(failed_map.items())[:20]}", flush=True)
        print("这些票不影响整体：下次运行会接着补（库里没有它们的记录，会重新拉）。", flush=True)

    # 退出码语义：
    #   0 = 有数据入库（允许少量票失败，它们下次自动补齐）
    #   3 = 一只都没成功，本次无意义
    #   4 = 被黑名单熔断
    # 早先的 "有失败就 3" 是错的：403 只失败会让上层判定整轮失败，
    # 于是 196 万行数据连同两小时一起被丢弃，缓存也没保存。
    if broken:
        return 4
    return 0 if ok > 0 else 3


if __name__ == "__main__":
    sys.exit(main())
