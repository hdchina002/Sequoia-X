"""策略基类模块：定义所有选股策略的抽象接口。"""

from abc import ABC, abstractmethod

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine


class BaseStrategy(ABC):
    """选股策略抽象基类。

    所有具体策略必须继承此类并实现 run() 方法。

    Attributes:
        display_name: 面向用户的中文策略名，用于飞书卡片标题与正文。
        criteria: 入选标准的一句话说明，直接展示在飞书卡片里，
            让人一眼看懂"这只票是因为符合什么条件才被选进来"。
        webhook_key: 策略对应的飞书 webhook 标识，用于路由到不同机器人。
            默认为 'default'，将使用 Settings.feishu_webhook_url。
            子类可覆盖此属性以路由到专属机器人，例如 'ma_volume'。
    """

    display_name: str = ""
    criteria: str = ""
    webhook_key: str = "default"
    # market: 'a' = A股（stock_daily 表），'hk' = 港股通（hk_daily 表）。
    # 同一个策略类可以被实例化两次分别跑两个市场，卡片标题靠 display_name 区分。
    market: str = "a"

    def __init__(self, engine: DataEngine, settings: Settings) -> None:
        """
        初始化策略。

        Args:
            engine: DataEngine 实例，用于读取行情数据。
            settings: Settings 实例，用于读取配置。
        """
        self.engine = engine
        self.settings = settings

    @abstractmethod
    def run(self) -> list[str]:
        """
        执行选股逻辑，返回选中的股票代码列表。

        Returns:
            满足策略条件的股票代码列表，如 ['000001', '600519']。
            无选股结果时返回空列表。
        """
        ...
