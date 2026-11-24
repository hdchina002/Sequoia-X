"""港股通数据同步：新浪名单 + 腾讯前复权日K → hk_daily 表。

为什么不用 baostock / 东财：
  - baostock 没有港股接口；
  - 东财 push2 容易触发 IP 限流（Push2RateLimitError，恢复要 5~40 分钟）。
因此名单走新浪（node=qbgg_hk = 港股通标的），K 线走腾讯 fqkline。

用法：
    python scripts/hk_sync.py --backfill          # 首次/全量：640 根日K
    python scripts/hk_sync.py --today-only        # 日常增量：最近 10 根
    python scripts/hk_sync.py --list-only         # 只更新港股通名单

纪律：
  - 所有请求走 curl 子进程（本机 python-requests 的 TLS 指纹会被行情站拒）；
  - 单只票失败只跳过该票，绝不中断整轮；
  - 写库用 INSERT OR REPLACE，重复跑不会产生重复行。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sequoia_v2.db"
LIST_PATH = ROOT / "data" / "hk_stock_list.json"

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS hk_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
)
"""
TABLE_INDEX = "CREATE INDEX IF NOT EXISTS idx_hk_symbol_date ON hk_daily (symbol, date)"

# 交易日滚动窗口：与 A 股 KEEP_DAYS 对齐，每天裁掉最老一天，库体积恒定
KEEP_DAYS = 400


def curl_json(url: str, timeout: int = 30):
    """用 curl 抓 JSON。本机 requests 会被行情站按 TLS 指纹拒，curl 稳定。

    带浏览器 UA 与 Referer：web.ifzq.gtimg.cn 在无 UA 高频请求下会触发腾讯 WAF
    （501page 拦截页），proxy.finance.qq.com 同路径更宽松，二者都带 UA 自保。
    """
    out = subprocess.run(
        [
            "curl", "-s", "-k", "--max-time", str(timeout),
            "-A", ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
            "-H", "Referer: https://gu.qq.com/",
            url,
        ],
        capture_output=True,
    )
    text = out.stdout.decode("utf-8", "ignore").strip()
    if not text or text.startswith("<!DOCTYPE"):
        return None  # WAF 拦截页 / 空包
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def fetch_ggt_universe() -> dict[str, str]:
    """港股通标的名单，返回 {5位代码: 中文名}。

    主源：东财数据中心「南向持股」（datacenter-web.eastmoney.com）——
      与 push2 行情服务器是两套服务，push2 被限流时它照常可用；
      一天约 667 只，正是港股通口径（有持股记录的标的）。
    备源：push2 板块接口 fs=b:DLMK0146,b:DLMK0144（akshare 同款参数）。

    踩过的坑（2026-11-24）：新浪 node=qbgg_hk 是「全部港股」约 2600 只，
      不是港股通。误用后 1800 只全量回填把腾讯 WAF 和东财同时打限速。
      名单总数若不在 300~2000 区间，直接放弃，绝不带错误名单回填。
    """
    if LIST_PATH.exists():
        age_days = (time.time() - LIST_PATH.stat().st_mtime) / 86400
        if age_days <= 7:
            cached = json.loads(LIST_PATH.read_text(encoding="utf-8"))
            if 300 <= len(cached) <= 2000:
                print(f"  用缓存名单（{age_days:.1f} 天前，{len(cached)} 只）")
                return {str(k): str(v) for k, v in cached.items()}

    result: dict[str, str] = {}

    # ── 主源：东财数据中心（不受 push2 限流影响）──
    try:
        import akshare as ak
        from datetime import date as _date
        from datetime import timedelta as _td

        end = _date.today()
        start = end - _td(days=7)
        df = ak.stock_hsgt_stock_statistics_em(
            symbol="南向持股",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
        if df is not None and len(df) and "股票代码" in df.columns:
            latest = df["持股日期"].max()
            df = df[df["持股日期"] == latest]
            for _, row in df.iterrows():
                code = str(row["股票代码"]).strip().zfill(5)
                name = str(row.get("股票简称", "")).strip()
                if code and name and code != "nan":
                    result[code] = name
            print(f"  [名单·数据中心] 持股日 {latest}，{len(result)} 只")
    except Exception as exc:  # noqa: BLE001
        print(f"  [名单·数据中心] 失败：{type(exc).__name__}: {exc}")

    # ── 备源：push2 板块接口（数据中心不可用时才碰，限流风险自负）──
    if not result:
        print("  [名单] 改走 push2 板块接口……")
        for pn in range(1, 12):
            url = (
                "https://push2.eastmoney.com/api/qt/clist/get"
                f"?pn={pn}&pz=100&po=1&np=1&fltt=2&invt=2&fs=b:DLMK0146,b:DLMK0144&fields=f12,f14"
            )
            data = curl_json(url)
            if not data:
                print(f"  [名单] 第 {pn} 页无响应（push2 限流），停止")
                break
            diff = (data.get("data") or {}).get("diff") or []
            if not diff:
                break
            for item in diff:
                code = str(item.get("f12", "")).strip().zfill(5)
                name = str(item.get("f14", "")).strip()
                if code and name:
                    result[code] = name
            print(f"  [名单] 第 {pn} 页 +{len(diff)}，累计 {len(result)}")
            time.sleep(1.0)

    if not (300 <= len(result) <= 2000):
        print(f"  名单数量异常（{len(result)}），不是港股通口径，放弃使用")
        return {}
    LIST_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    return result


def fetch_kline(code: str, count: int = 640) -> list[tuple]:
    """港股前复权日K，返回 [(date, open, high, low, close, volume, turnover), ...]。

    主源：新浪 `ak.stock_hk_hist_sina`（klc2_kl.js 静态文件，不限速）——
      一次给**全历史**（实测腾讯控股 5520 根 / 0.53 秒），且带**真实成交额 amount**，
      比腾讯准（腾讯日K只有成交量，成交额得用 volume×close 近似）。
      字段顺序 [date, open, high, low, close, volume, amount] 与 baostock 一致。
    备源：腾讯 proxy.finance.qq.com（新浪挂了才用；注意其字段顺序是
      [date, open, close, high, low, volume]，与 baostock 不同！）。
      不要用 web.ifzq.gtimg.cn——高频无 UA 会被腾讯 WAF 拦成 501page。
    """
    out: list[tuple] = []

    # ── 主源：新浪 ──
    try:
        import akshare as ak

        df = ak.stock_hk_hist_sina(symbol=code, period="daily", adjust="qfq")
        if df is not None and len(df):
            for _, row in df.iterrows():
                try:
                    d = str(row["date"])[:10]
                    o, h = float(row["open"]), float(row["high"])
                    l, c = float(row["low"]), float(row["close"])
                    v = float(row["volume"])
                    amt = float(row.get("amount", 0) or 0)
                except (TypeError, ValueError, KeyError):
                    continue
                if v <= 0 or c <= 0:
                    continue
                out.append((d, o, h, l, c, v, round(amt if amt > 0 else v * c, 2)))
            if out:
                return out[-count:] if count else out
    except Exception as exc:  # noqa: BLE001
        print(f"  [{code}] 新浪日K失败：{type(exc).__name__}: {exc}")

    # ── 备源：腾讯 proxy ──
    url = (
        "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get?"
        f"param=hk{code},day,,,{count},qfq"
    )
    data = curl_json(url)
    if not data:
        return []
    node = (data.get("data") or {}).get(f"hk{code}") or {}
    rows = node.get("day") or node.get("qfqday") or []
    for r in rows:
        if len(r) < 6:
            continue
        try:
            d = str(r[0])
            o, c, h, l = float(r[1]), float(r[2]), float(r[3]), float(r[4])
            v = float(r[5])
        except (TypeError, ValueError):
            continue
        if v <= 0 or c <= 0:
            continue  # 停牌空值，跟 A 股一样不入库
        out.append((d, o, h, l, c, v, round(v * c, 2)))
    return out


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(TABLE_DDL)
    conn.execute(TABLE_INDEX)


def write_rows(conn: sqlite3.Connection, symbol: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn.executemany(
        "INSERT OR REPLACE INTO hk_daily "
        "(symbol, date, open, high, low, close, volume, turnover) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(symbol, *r) for r in rows],
    )
    return len(rows)


def trim_history(conn: sqlite3.Connection, keep_days: int = KEEP_DAYS) -> int:
    """只保留最近 keep_days 个交易日，与 A 股 trim_history 口径一致。"""
    cur = conn.execute(
        "SELECT DISTINCT date FROM hk_daily ORDER BY date DESC LIMIT ?",
        (keep_days,),
    )
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < keep_days:
        return 0
    cutoff = dates[-1]
    cur = conn.execute("DELETE FROM hk_daily WHERE date < ?", (cutoff,))
    return cur.rowcount or 0


def main() -> int:
    parser = argparse.ArgumentParser(description="港股通数据同步")
    parser.add_argument("--backfill", action="store_true", help="全量回填 640 根日K")
    parser.add_argument("--today-only", action="store_true", help="增量：只拉最近 10 根")
    parser.add_argument("--list-only", action="store_true", help="只更新港股通名单")
    parser.add_argument("--workers", type=int, default=3, help="并发数（腾讯较宽松，默认 3）")
    parser.add_argument("--limit", type=int, default=0, help="只处理前 N 只（调试用，0=全部）")
    args = parser.parse_args()

    print("拉取港股通标的名单（东财，带 7 天缓存）……")
    universe = fetch_ggt_universe()
    if not universe:
        print("名单为空（东财限流且无缓存），稍后重试")
        return 1
    print(f"港股通标的 {len(universe)} 只")

    if args.list_only:
        return 0

    count = 10 if args.today_only else 640
    print(f"拉取港股日K（腾讯，每只 {count} 根，{args.workers} 并发）……")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    ensure_table(conn)

    ok = fail = skipped = 0
    codes = sorted(universe)
    if args.limit:
        codes = codes[: args.limit]
    # 串行为主：腾讯容忍度高，但并发太高仍可能被 WAF 限速
    for i, code in enumerate(codes, 1):
        rows = fetch_kline(code, count)
        if not rows:
            fail += 1
            continue
        try:
            n = write_rows(conn, code, rows)
            conn.commit()
            if n:
                ok += 1
            else:
                skipped += 1
        except sqlite3.Error as exc:
            print(f"  [{code}] 写库失败：{exc}")
            fail += 1
        if i % 50 == 0:
            print(f"  进度 {i}/{len(codes)}（成功 {ok} 失败 {fail}）")
        time.sleep(0.35)  # 腾讯 WAF 对高频敏感，0.35s 起步，宁慢勿断

    dropped = trim_history(conn)
    # 必须提交：python sqlite3 在 close() 时会回滚未提交事务，
    # 不 commit 等于白删（实测 rowcount 显示删了 129047 行，库里却还在）。
    conn.commit()
    conn.close()

    print(
        f"完成：成功 {ok} / 失败 {fail} / 空 {skipped}；"
        f"滚动裁剪 {dropped} 行（保留最近 {KEEP_DAYS} 个交易日）"
    )
    # 有数据入库即视为成功（与 A 股回填退出码语义一致：允许少量票失败）
    return 0 if ok > 0 else 3


if __name__ == "__main__":
    sys.exit(main())
