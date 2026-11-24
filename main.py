"""Sequoia-X V2 主程序入口。

两种运行模式：
  python main.py               # 日常模式：8进程增量补数据 + 跑策略 + 飞书推送（2~3分钟）
  python main.py --backfill    # 回填模式：baostock 拉全市场历史K线（首次/补数据用，约12分钟）
"""

import argparse
import sys
from dotenv import load_dotenv
load_dotenv()

from datetime import date

import socket
socket.setdefaulttimeout(10.0)

from sequoia_x.core.config import get_settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.feishu import FeishuNotifier
from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy
from sequoia_x.strategy.limit_up_shakeout import LimitUpShakeoutStrategy
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy
from sequoia_x.strategy.uptrend_limit_down import UptrendLimitDownStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy
from sequoia_x.strategy.private_placement import PrivatePlacementStrategy


def main() -> None:
    parser = argparse.ArgumentParser(description="Sequoia-X V2 选股系统")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="回填模式：通过 baostock 拉取全市场历史 K 线（约12分钟）",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="跳过本次的行情同步（数据已由上层调度器拉好时使用，避免重复请求 baostock）",
    )
    args = parser.parse_args()

    try:
        # 1. 初始化配置
        settings = get_settings()

        # 2. 初始化日志
        logger = get_logger(__name__)
        logger.info("Sequoia-X V2 启动")

        # 3. 初始化数据引擎
        engine = DataEngine(settings)

        if args.backfill:
            # ── 回填模式：单线程保守拉历史 K 线，自动多轮重跑 ──
            logger.info("进入回填模式...")
            all_symbols = engine.get_all_symbols()
            engine.backfill(all_symbols)
            logger.info("Sequoia-X V2 回填模式运行完成")
            return

        # ── 日常模式：单次 API 补今天 + 策略 + 推送 ──
        if args.skip_sync:
            # [本地补丁] 由 scripts/daily_run.py 用 3 进程的并行脚本先同步好，
            # 这里再调一次 sync_today_bulk（8 进程 × 各自 login）纯属重复劳动，
            # 且是触发 baostock 黑名单的主要来源。加开关避免双份请求。
            logger.info("跳过快照同步（数据已由调度器同步）")
        else:
            logger.info("开始拉取最新快照...")
            count = engine.sync_today_bulk()
            logger.info(f"快照同步完成，写入 {count} 只股票")

        # 4. 策略列表（新增策略在此追加即可）
        strategies: list[BaseStrategy] = [
            MaVolumeStrategy(engine=engine, settings=settings),
            TurtleTradeStrategy(engine=engine, settings=settings),
            HighTightFlagStrategy(engine=engine, settings=settings),
            LimitUpShakeoutStrategy(engine=engine, settings=settings),
            UptrendLimitDownStrategy(engine=engine, settings=settings),
            RpsBreakoutStrategy(engine=engine, settings=settings),
            PrivatePlacementStrategy(engine=engine, settings=settings),
        ]

        # ── 港股通：只放行与涨跌停/定增无关的四个策略 ──
        # 涨停后洗盘、趋势中跌停错杀（港股没有涨跌停板）、
        # 定增公告（港股对应的是配售，规则完全不同）都不适用。
        # 同一策略类再实例化一次跑港股通，靠 market 字段切到 hk_daily 表。
        for cls in (
            MaVolumeStrategy,
            TurtleTradeStrategy,
            HighTightFlagStrategy,
            RpsBreakoutStrategy,
        ):
            hk_inst = cls(engine=engine, settings=settings)
            hk_inst.market = "hk"
            hk_inst.display_name = f"港股通 · {cls.display_name}"
            hk_inst.criteria = cls.criteria + "（在港股通标的内筛选）"
            strategies.append(hk_inst)

        notifier = FeishuNotifier(settings)

        # 5. 遍历策略，有结果则推送至对应机器人
        push_failed = 0
        for strategy in strategies:
            strategy_name = type(strategy).__name__
            display_name = strategy.display_name or strategy_name
            logger.info(f"执行策略：{display_name}（{strategy_name}）")

            # 单个策略崩了不能拖垮其余 6 个，也不能让推送整体中断。
            try:
                selected: list[str] = strategy.run()
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    f"{strategy_name} 执行异常，跳过该策略：{type(exc).__name__}: {exc}"
                )
                continue
            logger.info(f"{strategy_name} 选出 {len(selected)} 只股票")

            if selected:
                try:
                    ok = notifier.send(
                        symbols=selected,
                        strategy_name=strategy.display_name or strategy_name,
                        webhook_key=strategy.webhook_key,
                        criteria=strategy.criteria,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        f"{strategy_name} 推送异常：{type(exc).__name__}: {exc}"
                    )
                    ok = False
                if not ok:
                    push_failed += 1
            else:
                logger.info(f"{strategy_name} 无选股结果，跳过推送")

        # [本地补丁] 任一推送失败则以非 0 退出，供上层调度器判定"本次未成功"从而重试
        if push_failed:
            logger.error(f"共有 {push_failed} 个策略推送失败，返回退出码 2")
            sys.exit(2)

    except Exception:
        try:
            _logger = get_logger(__name__)
            _logger.exception("主流程发生未捕获异常，程序终止")
        except Exception:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    logger.info("Sequoia-X V2 运行完成")


if __name__ == "__main__":
    main()
