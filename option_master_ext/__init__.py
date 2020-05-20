from pathlib import Path
from vnpy.trader.app import BaseApp
from .engine_ext import OptionEngineExt, APP_NAME


class OptionMasterExtApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "期权交易扩展版"
    engine_class = OptionEngineExt
    widget_name = "OptionManagerExt"
    icon_name = "option_ext.ico"
