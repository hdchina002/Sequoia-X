"""飞书通知模块：将选股结果通过 Webhook 推送至飞书群。"""

import json
from datetime import date

import requests

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


class FeishuNotifier:
    """飞书 Webhook 推送器。

    根据策略的 webhook_key 路由到对应的飞书机器人。
    若 webhook_key 未在 Settings.strategy_webhooks 中配置，
    则 fallback 到 Settings.feishu_webhook_url。
    """

    def __init__(self, settings: Settings) -> None:
        """
        初始化 FeishuNotifier。

        Args:
            settings: Settings 实例，提供 Webhook URL 配置。
        """
        self.settings = settings

    @staticmethod
    def _to_xueqiu_code(code: str) -> str:
        """将纯数字代码转为雪球格式：6开头→SH，4/8开头→BJ，其余→SZ。"""
        if code.startswith("6"):
            return f"SH{code}"
        elif code.startswith(("4", "8")):
            return f"BJ{code}"
        return f"SZ{code}"

    # 全市场「代码→名称」映射，进程内只拉一次；7 个策略共用，避免重复请求。
    _NAME_CACHE: dict[str, str] = {}
    _NAME_TRIED = False

    @classmethod
    def _get_stock_names(cls, symbols: list[str]) -> dict[str, str]:
        """一次性拉全市场名称映射；数据源异常时返回已知部分，绝不抛异常。

        原实现逐只票调用 query_stock_basic —— 选中 16 只就发 16 次请求。
        baostock 在 GitHub Actions（Azure 出境）长连接不稳，常返回空包，
        触发 IndexError 把整个 main.py 带崩。实测 2026-11-23 因此一条消息都没发出去。
        名称只是卡片的锦上添花，拿不到就显示 SH600000 / SZ000001，不影响推送。
        """
        if cls._NAME_TRIED:
            return cls._NAME_CACHE
        cls._NAME_TRIED = True
        try:
            import socket

            import baostock as bs

            old_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(8)
            try:
                lg = bs.login()
                if lg.error_code != "0":
                    logger.warning(f"股票名称查询登录失败：{lg.error_msg}")
                    return cls._NAME_CACHE
                rs = bs.query_stock_basic()  # 不带 code = 一次拿全市场
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 2 and "." in row[0]:
                        cls._NAME_CACHE[row[0].split(".")[-1]] = row[1]
                bs.logout()
                logger.info(f"股票名称映射加载 {len(cls._NAME_CACHE)} 条")
            finally:
                socket.setdefaulttimeout(old_timeout)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"股票名称获取失败，卡片将显示代码：{type(exc).__name__}: {exc}"
            )
        return cls._NAME_CACHE

    def _build_card(self, symbols: list[str], strategy_name: str) -> dict:
        today = date.today().strftime("%Y-%m-%d")
        names = self._get_stock_names(symbols)

        links: list[str] = []
        for code in symbols:
            xq_code = self._to_xueqiu_code(code)
            name = names.get(code, xq_code)
            links.append(f"[{name}](https://xueqiu.com/S/{xq_code})")

        symbol_text = " ".join(links) if links else "（无选股结果）"

        return {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"📈 Sequoia-X 选股播报 | {strategy_name}",
                    },
                    "template": "blue",
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**日期：** {today}\n**策略：** {strategy_name}\n**选股数量：** {len(symbols)}",
                        },
                    },
                    {"tag": "hr"},
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**选股列表：**\n{symbol_text}",
                        },
                    },
                ],
            },
        }

    def send(
        self,
        symbols: list[str],
        strategy_name: str,
        webhook_key: str = "default",
    ) -> bool:
        """
        将选股结果格式化为飞书卡片消息并 POST 至对应 Webhook。

        根据 webhook_key 从 Settings 中查找专属 URL；
        若未配置，则 fallback 到 feishu_webhook_url。

        Args:
            symbols: 选股结果代码列表。
            strategy_name: 策略名称，用于卡片标题。
            webhook_key: 策略标识，用于路由到对应飞书机器人。

        Raises:
            不抛出异常，HTTP 失败时记录 ERROR 日志。
        """
        url = self.settings.get_webhook_url(webhook_key)
        try:
            payload = self._build_card(symbols, strategy_name)
        except Exception as exc:  # noqa: BLE001
            # 卡片构建失败也要把结果发出去：降级为纯文本，保证不因显示层问题漏推。
            logger.error(f"卡片构建失败，降级为纯文本：{type(exc).__name__}: {exc}")
            payload = {
                "msg_type": "text",
                "content": {
                    "text": (
                        f"Sequoia-X 选股播报 | {strategy_name}\n"
                        f"共 {len(symbols)} 只：{' '.join(symbols)}"
                    )
                },
            }

        try:
            resp = requests.post(
                url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            # 解析飞书真正的返回体
            resp_json = resp.json()

            # 飞书真正的成功标志是内部的 code == 0
            if resp.status_code != 200 or resp_json.get("code") != 0:
                logger.error(
                    f"飞书推送失败 [{webhook_key}] "
                    f"HTTP状态={resp.status_code} 飞书响应={resp.text}"
                )
                return False
            else:
                logger.info(f"飞书推送成功 [{webhook_key}]，共 {len(symbols)} 只股票")
                return True

        except Exception as exc:  # noqa: BLE001
            # 不只拦 requests 异常：飞书返回异常 JSON、编码问题等也不能让主流程崩。
            logger.error(f"飞书推送异常 [{webhook_key}]：{type(exc).__name__}: {exc}")
            return False
