from pathlib import Path

from vnpy.trader.app import BaseApp

from .engine import FollowEngine, APP_NAME


class FollowTradingApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "跟随交易"
    engine_class = FollowEngine
    widget_name = "FollowManager"
    icon_name = "follow.ico"
