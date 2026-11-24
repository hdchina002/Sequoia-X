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
        """将纯数字代码转为雪球格式：5位→HK，6开头→SH，4/8/9开头→BJ，其余→SZ。"""
        if len(code) == 5:
            return f"HK{code}"
        if code.startswith("6"):
            return f"SH{code}"
        if code.startswith(("4", "8", "9")):
            return f"BJ{code}"
        return f"SZ{code}"

    @staticmethod
    def _market_label(code: str) -> str:
        """代码→市场中文前缀，名称拿不到时降级显示成「深 002749」而不是 SZ002749。"""
        if len(code) == 5:
            return "港"
        if code.startswith("6"):
            return "沪"
        if code.startswith(("4", "8", "9")):
            return "京"
        return "深"

    @classmethod
    def _fetch_names_from_tencent(cls, codes: list[str]) -> dict[str, str]:
        """用腾讯行情快照批量补股票名称（baostock 拿不到时的兜底）。

        返回 {6位代码: 中文名}。每批最多 50 只（腾讯 URL 过长会 414），
        单批失败只影响该批，绝不抛异常。
        """
        result: dict[str, str] = {}
        if not codes:
            return result
        for i in range(0, len(codes), 50):
            batch = codes[i : i + 50]
            query = ",".join(cls._to_xueqiu_code(c).lower() for c in batch)
            try:
                resp = requests.get(f"https://qt.gtimg.cn/q={query}", timeout=8)
                text = resp.content.decode("gbk", errors="ignore")
                for line in text.splitlines():
                    if '="' not in line:
                        continue
                    payload = line.split("=", 1)[1].strip().strip('";')
                    fields = payload.split("~")
                    if len(fields) >= 3 and fields[1] and fields[2]:
                        result[fields[2]] = fields[1]
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"腾讯名称补全批次失败：{type(exc).__name__}: {exc}")
        return result

    # 全市场「代码→名称」映射，进程内只拉一次；7 个策略共用，避免重复请求。
    _NAME_CACHE: dict[str, str] = {}
    _NAME_TRIED = False

    @classmethod
    def _get_stock_names(cls, symbols: list[str]) -> dict[str, str]:
        """取「代码→名称」映射；数据源异常时返回已知部分，绝不抛异常。

        原实现逐只票调用 query_stock_basic —— 选中 16 只就发 16 次请求。
        baostock 在 GitHub Actions（Azure 出境）长连接不稳，常返回空包，
        触发 IndexError 把整个 main.py 带崩。实测 2026-11-23 因此一条消息都没发出去。

        兜底链：baostock 全市场（进程内缓存一次）→ 腾讯快照只补本次用到的缺票。
        两层都拿不到就降级显示「深 002749」，不影响推送。
        """
        if not cls._NAME_TRIED:
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
                    else:
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
                    f"股票名称获取失败，改用腾讯接口补：{type(exc).__name__}: {exc}"
                )

        # 腾讯快照补缺：只补本次推送真正用到、且 baostock 没给到的票
        missing = [s for s in symbols if s and s not in cls._NAME_CACHE]
        if missing:
            filled = cls._fetch_names_from_tencent(missing)
            cls._NAME_CACHE.update(filled)
            logger.info(f"腾讯接口补名称 {len(filled)}/{len(missing)} 条")
        return cls._NAME_CACHE

    def _build_card(
        self,
        symbols: list[str],
        strategy_name: str,
        criteria: str = "",
        force_flat: bool = False,
    ) -> dict:
        today = date.today().strftime("%Y-%m-%d")
        names = self._get_stock_names(symbols)

        links: list[str] = []
        for code in symbols:
            # 港股（5位）雪球网页 URL 用纯数字不带 HK 前缀（HK00700 是 API 格式，网页会报错）；
            # A股用 SH/SZ/BJ 前缀。
            url_code = code if len(code) == 5 else self._to_xueqiu_code(code)
            # 名称兜底链：baostock/腾讯名称 → 「深 002749」这种中文市场前缀
            name = names.get(code) or f"{self._market_label(code)} {code}"
            links.append(f"[{name}](https://xueqiu.com/S/{url_code})")

        info_lines = [
            f"**日期：** {today}",
            f"**策略：** {strategy_name}",
        ]
        if criteria:
            info_lines.append(f"**入选标准：** {criteria}")
        info_lines.append(f"**选股数量：** {len(symbols)}")

        _MAX_VISIBLE = 20  # 超过 20 只时折叠：前 20 直接可见，其余点「展开」看

        # —— 少于等于 20 只：全量平铺（老格式，兼容性最好）——
        if force_flat or len(links) <= _MAX_VISIBLE:
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
                                "content": "\n".join(info_lines),
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

        # —— 超过 20 只：前 20 平铺 + collapsible_panel 折叠其余（卡片 2.0）——
        visible_text = " ".join(links[:_MAX_VISIBLE])
        hidden_text = " ".join(links[_MAX_VISIBLE:])
        hidden_count = len(links) - _MAX_VISIBLE
        return {
            "msg_type": "interactive",
            "card": {
                "schema": "2.0",
                "config": {"update_multi": True},
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"📈 Sequoia-X 选股播报 | {strategy_name}",
                    },
                    "template": "blue",
                },
                "body": {
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": "\n".join(info_lines),
                            },
                        },
                        {"tag": "hr"},
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": f"**选股列表（前 {_MAX_VISIBLE} 只）：**\n{visible_text}",
                            },
                        },
                        {
                            "tag": "collapsible_panel",
                            "expanded": False,
                            "header": {
                                "title": {
                                    "tag": "plain_text",
                                    "content": f"展开剩余 {hidden_count} 只（点击展开）",
                                },
                            },
                            "elements": [
                                {
                                    "tag": "div",
                                    "text": {
                                        "tag": "lark_md",
                                        "content": hidden_text,
                                    },
                                },
                            ],
                        },
                    ],
                },
            },
        }

    def send(
        self,
        symbols: list[str],
        strategy_name: str,
        webhook_key: str = "default",
        criteria: str = "",
    ) -> bool:
        """
        将选股结果格式化为飞书卡片消息并 POST 至对应 Webhook。

        根据 webhook_key 从 Settings 中查找专属 URL；
        若未配置，则 fallback 到 feishu_webhook_url。

        Args:
            symbols: 选股结果代码列表。
            strategy_name: 策略中文名，用于卡片标题与正文。
            webhook_key: 策略标识，用于路由到对应飞书机器人。
            criteria: 入选标准的一句话说明，展示在卡片里。

        Raises:
            不抛出异常，HTTP 失败时记录 ERROR 日志。
        """
        url = self.settings.get_webhook_url(webhook_key)
        try:
            payload = self._build_card(symbols, strategy_name, criteria)
        except Exception as exc:  # noqa: BLE001
            # 卡片构建失败也要把结果发出去：降级为纯文本，保证不因显示层问题漏推。
            logger.error(f"卡片构建失败，降级为纯文本：{type(exc).__name__}: {exc}")
            payload = {
                "msg_type": "text",
                "content": {
                    "text": (
                        f"Sequoia-X 选股播报 | {strategy_name}\n"
                        f"入选标准：{criteria}\n"
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

            # schema 2.0 的 collapsible_panel 若飞书不支持，降级重发（全量平铺）
            if (
                resp.status_code != 200 or resp_json.get("code") != 0
            ) and payload.get("card", {}).get("schema") == "2.0":
                logger.warning(
                    f"卡片 2.0 schema 发送失败，降级为平铺列表重发："
                    f"HTTP={resp.status_code} 响应={resp.text[:200]}"
                )
                payload = self._build_card(
                    symbols, strategy_name, criteria, force_flat=True
                )
                resp = requests.post(
                    url,
                    data=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
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
