from typing import List, Dict

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtWidgets, QtCore, QtGui

from vnpy.app.option_master.ui.monitor import MonitorCell, PosCell
from vnpy.app.option_master.ui.manager import AlgoSpinBox

from ..engine_ext import (
    APP_NAME, EVENT_OPTION_HEDGE_STATUS,
    OptionEngineExt, ChannelHedgeEngine
)


class HedgeChainCombo(QtWidgets.QComboBox):
    def __init__(self, portfolio_name: str, monitor: "ChannelHedgeMonitor"):
        super().__init__()
        self.portfolio_name = portfolio_name
        self.monitor = monitor

    def get_value(self) -> str:
        return self.currentText()


class HedgeAutoButton(QtWidgets.QPushButton):
    def __init__(self, portfolio_name: str, monitor: "ChannelHedgeMonitor"):
        super().__init__()
        self.portfolio_name = portfolio_name
        self.monitor = monitor

        self.active = False
        self.setText("OFF")
        self.clicked.connect(self.on_clicked)

    def on_clicked(self) -> None:
        if self.active:
            self.monitor.stop_auto_hedge(self.portfolio_name)
        else:
            self.monitor.start_auto_hedge(self.portfolio_name)

    def update_status(self, active: bool) -> None:
        self.active = active

        if active:
            self.setText("ON")
        else:
            self.setText("OFF")


class HedgeActionButton(QtWidgets.QPushButton):
    def __init__(self, portfolio_name: str, monitor: "ChannelHedgeMonitor"):
        super().__init__()
        self.portfolio_name = portfolio_name
        self.monitor = monitor


class HedgePercentSpinBox(AlgoSpinBox):
    def __init__(self):
        super().__init__()
        self.setMaximum(100)
        self.setMinimum(10)
        self.setSingleStep(10)

    def get_value(self) -> float:
        return self.value() / 100


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
    signal_status = QtCore.pyqtSignal(Event)

    headers: List[Dict] = [
        {"name": "portfolio_name", "display": "组合名称", "cell": MonitorCell},
        {"name": "balance", "display": "中性基准价", "cell": MonitorCell},
        {"name": "up_threshold", "display": "上阈值", "cell": MonitorCell},
        {"name": "down_threshold", "display": "下阈值", "cell": MonitorCell},
        {"name": "pos_delta", "display": "Delta偏移量", "cell": MonitorCell},
        {"name": "net_pos", "display": "组合净仓", "cell": PosCell},
        {"name": "chain_symbol", "display": "对冲期权链", "cell": HedgeChainCombo},
        {"name": "offset_percent", "display": "偏移比例", "cell": HedgePercentSpinBox},
        {"name": "hedge_percent", "display": "对冲比例", "cell": HedgePercentSpinBox},
        {"name": "auto_hedge", "display": "自动对冲", "cell": HedgeAutoButton},
        {"name": "action_hedge", "display": "立即对冲", "cell": HedgeActionButton},
    ]

    def __init__(self, option_engine: OptionEngineExt):
        super().__init__()

        self.option_engine: OptionEngineExt = option_engine
        self.event_engine: EventEngine = option_engine.event_engine
        self.hedge_engine: ChannelHedgeEngine = self.option_engine.channel_hedge_engine

        self.cells: Dict[str, Dict] = {}

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        self.setWindowTitle("通道对冲")
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(self.NoEditTriggers)

        portfolio_names = self.option_engine.get_portfolio_names()
        self.setRowCount(len(portfolio_names))
        self.setColumnCount(len(self.headers))

        labels = [d["display"] for d in self.headers]
        self.setHorizontalHeaderLabels(labels)

        for row, portfolio_name in enumerate(self.option_engine.portfolios):
            portfolio_cells = {}
            for column, d in enumerate(self.headers):
                cell_type  = d['cell']
                cell_name = d['name']

                if cell_name in ['chain_symbol', 'auto_hedge', 'action_hedge']:
                    cell = cell_type(portfolio_name, self)
                else:
                    cell = cell_type()

                if isinstance(cell, QtWidgets.QTableWidgetItem):
                    self.setItem(row, column, cell)
                else:
                    self.setCellWidget(row, column, cell)

                portfolio_cells[cell_name] = cell
            
            self.cells[portfolio_name] = portfolio_cells

        self.resizeColumnsToContents()

        for portfolio_name in self.cells:
            self.update_balance_price(portfolio_name)
            self.update_portfolio_attr(portfolio_name, 'net_pos')
            self.update_portfolio_attr(portfolio_name, 'pos_delta')

    def register_event(self) -> None:
        self.signal_status.connect(self.process_status_event)
        self.event_engine.register(EVENT_OPTION_HEDGE_STATUS, self.signal_status.emit)

    def process_status_event(self, event: Event) -> None:
        status_dict = event.data
        for portfolio_name in self.option_engine.portfolios:
            cells = self.cells[portfolio_name]
            cells['auto_hedge'].update_status(status_dict[portfolio_name])

    def update_balance_price(self, portfolio_name: str):
        price = self.hedge_engine.get_balance_price(portfolio_name)
        cells = self.cells[portfolio_name]
        cells['balance_price'].setText(str(price))

    def update_portfolio_attr(self, portfolio_name: str, attr_name: str):
        portfolio = self.option_engine.get_portfolio(portfolio_name)
        cells = self.cells[portfolio_name]

        if attr_name in cells:
            value = getattr(portfolio, attr_name, None)
            if value:
                cells['net_pos'].setText(str(value))
    
    def start_auto_hedge(self, portfolio_name) -> None:
        cells = self.cells[portfolio_name]
        params = {}
        for name in ['chain_symobl', 'offset_percent', 'hedge_percent']:
            params[name] = cells[name].get_value()

        self.hedge_engine.start_auto_hedge(portfolio_name, params)

    def stop_auto_hedge(self, portfolio_name) -> None:
        self.hedge_engine.stop_auto_hedge(portfolio_name)


