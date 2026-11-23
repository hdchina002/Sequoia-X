"""本地预览：跑完 7 个策略但**不推送**，只在终端打印结果。

用途：
  1. 装好之后先确认策略真的能出票（不用等飞书配置好）
  2. 想复核"今天推了什么"时随时看
  3. 排查策略是否异常（某个策略长期 0 只票 = 可疑）

用法：
  .venv/Scripts/python.exe scripts/preview.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from rich.console import Console  # noqa: E402

from sequoia_x.core.config import get_settings  # noqa: E402
from sequoia_x.data.engine import DataEngine  # noqa: E402
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy  # noqa: E402
from sequoia_x.strategy.limit_up_shakeout import LimitUpShakeoutStrategy  # noqa: E402
from sequoia_x.strategy.ma_volume import MaVolumeStrategy  # noqa: E402
from sequoia_x.strategy.private_placement import PrivatePlacementStrategy  # noqa: E402
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy  # noqa: E402
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy  # noqa: E402
from sequoia_x.strategy.uptrend_limit_down import UptrendLimitDownStrategy  # noqa: E402

console = Console()


def main() -> None:
    settings = get_settings()
    engine = DataEngine(settings)

    strategies = [
        MaVolumeStrategy(engine=engine, settings=settings),
        TurtleTradeStrategy(engine=engine, settings=settings),
        HighTightFlagStrategy(engine=engine, settings=settings),
        LimitUpShakeoutStrategy(engine=engine, settings=settings),
        UptrendLimitDownStrategy(engine=engine, settings=settings),
        RpsBreakoutStrategy(engine=engine, settings=settings),
        PrivatePlacementStrategy(engine=engine, settings=settings),
    ]

    console.rule("[bold]Sequoia-X 策略预览（不推送）")
    total = 0
    for strategy in strategies:
        name = type(strategy).__name__
        try:
            selected = strategy.run()
        except Exception as exc:  # noqa: BLE001
            console.print(f"[red]{name} 执行异常：{exc}[/red]")
            continue
        total += len(selected)
        if selected:
            console.print(f"[green]{name}[/green] 选出 {len(selected)} 只：{' '.join(selected)}")
        else:
            console.print(f"[dim]{name} 无结果[/dim]")

    console.rule(f"合计 {total} 条（去重前）")


if __name__ == "__main__":
    main()
