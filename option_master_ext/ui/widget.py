from typing import List, Dict

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtWidgets, QtCore, QtGui

from vnpy.app.option_master.ui.monitor import MonitorCell, PosCell
from vnpy.app.option_master.ui.manager import AlgoSpinBox

from ..engine_ext import APP_NAME, OptionEngineExt

class HedgeChainCombo(QtWidgets.QComboBox):
    def __init__(self, monitor: "ChannelHedgeMonitor"):
        super().__init__()
        pass


class HedgeAutoButton(QtWidgets.QPushButton):
    def __init__(self, monitor: "ChannelHedgeMonitor"):
        super().__init__()
        pass


class HedgeActionButton(QtWidgets.QPushButton):
    def __init__(self, monitor: "ChannelHedgeMonitor"):
        super().__init__()
        pass


class HedgePercentSpinBox(AlgoSpinBox):
    def __init__(self):
        super().__init__()

        self.setMaximum(100)
        self.setMinimum(0)


class OptionManagerExt(QtWidgets.QWidget):

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__()

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.option_engine = main_engine.get_engine(APP_NAME)

        self.hedge_manager: ChannelHedgeMonitor = None
        self.volatility_trading = None

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("OptionMasterExt")

        self.volatility_button = QtWidgets.QPushButton("波动率交易")
        self.hedge_button = QtWidgets.QPushButton("Delta对冲")

        for button in [
            self.volatility_button,
            self.hedge_button,
        ]:
            button.setEnabled(False)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(self.volatility_button)
        hbox.addWidget(self.hedge_button)

        self.setLayout(hbox)

    def register_event(self) -> None:
        pass


class ChannelHedgeMonitor(QtWidgets.QTableWidget):
    headers: List[Dict] = [
        {"name": "portfolio_name", "display": "组合名称", "cell": MonitorCell},
        {"name": "balance", "display": "中性基准价", "cell": MonitorCell},
        {"name": "up_threshold", "display": "上阈值", "cell": MonitorCell},
        {"name": "down_threshold", "display": "下阈值", "cell": MonitorCell},
        {"name": "delta_offset", "display": "Delta偏移量", "cell": MonitorCell},
        {"name": "net_pos", "display": "组合净仓", "cell": PosCell},
        {"name": "chain_symbol", "display": "对冲期权链", "cell": HedgeChainCombo},
        {"name": "hedge_percent", "display": "对冲比例", "cell": HedgePercentSpinBox},
        {"name": "offset_percent", "display": "偏移比例", "cell": HedgePercentSpinBox},
        {"name": "chain_symbol", "display": "自动对冲", "cell": HedgeAutoButton},
        {"name": "chain_symbol", "display": "立即对冲", "cell": HedgeActionButton},
    ]

    def __init__(self, option_engine: OptionEngineExt):
        super().__init__()

        self.option_engine: OptionEngineExt = option_engine
        self.event_engine: EventEngine = option_engine.event_engine

        self.init_ui()


    def init_ui(self) -> None:
        self.setWindowTitle("通道对冲")
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(self.NoEditTriggers)

        portfolio_names = self.option_engine.get_portfolio_names()
        self.setRowCount(len(portfolio_names))
        self.setColumnCount(len(self.headers))

        labels = [d["display"] for d in self.headers]
        self.setHorizontalHeaderLabels(labels)