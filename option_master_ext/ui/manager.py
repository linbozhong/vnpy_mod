from vnpy.event import EventEngine, Event

from vnpy.trader.ui import QtWidgets, QtCore, QtGui

from ..engine_ext import (
    EVENT_OPTION_HEDGE_ALGO_LOG,
    OptionEngineExt, HedgeEngine, ChannelHedgeAlgo
)
from .monitor import (
    OffsetPercentSpinBox, HedgePercentSpinBox,
    HedgeMonitor, StrategyOrderMonitor
)

class HedgeManager(QtWidgets.QWidget):

    signal_log = QtCore.pyqtSignal(Event)

    def __init__(self, option_engine: OptionEngineExt):
        super().__init__()
        self.option_engine = option_engine
        self.main_engine = option_engine.main_engine
        self.event_engine = option_engine.event_engine
        self.hedge_engine = option_engine.hedge_engine

        self.hedge_engine.init_engine()

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        self.setWindowTitle("Delta对冲")
        self.setMaximumSize(1440, 800)

        self.hedge_monitor = HedgeMonitor(self.option_engine)
        self.strategy_order_monitor = StrategyOrderMonitor(self.main_engine, self.event_engine)

        self.log_monitor = QtWidgets.QTextEdit()
        self.log_monitor.setReadOnly(True)
        self.log_monitor.setMaximumWidth(300)

        start_hedge_button = QtWidgets.QPushButton("全部启动")
        start_hedge_button.clicked.connect(self.start_for_all)

        stop_hedge_button = QtWidgets.QPushButton("全部停止")
        stop_hedge_button.clicked.connect(self.stop_for_all)

        self.offset_percent = OffsetPercentSpinBox()
        self.hedge_percent = HedgePercentSpinBox()

        offset_percent_btn = QtWidgets.QPushButton("设置")
        offset_percent_btn.clicked.connect(self.set_offset_percent)

        hedge_percent_btn = QtWidgets.QPushButton("设置")
        hedge_percent_btn.clicked.connect(self.set_hedge_percent)

        QLabel = QtWidgets.QLabel
        grid = QtWidgets.QGridLayout()
        grid.addWidget(QLabel("偏移比例"), 0, 0)
        grid.addWidget(self.offset_percent, 0, 1)
        grid.addWidget(offset_percent_btn, 0, 2)
        grid.addWidget(QLabel("对冲比例"), 1, 0)
        grid.addWidget(self.hedge_percent, 1, 1)
        grid.addWidget(hedge_percent_btn, 1, 2)

        left_vbox = QtWidgets.QVBoxLayout()
        left_vbox.addWidget(self.hedge_monitor)
        left_vbox.addWidget(self.strategy_order_monitor)

        ctrl_btn_hbox = QtWidgets.QHBoxLayout()
        ctrl_btn_hbox.addWidget(start_hedge_button)
        ctrl_btn_hbox.addWidget(stop_hedge_button)

        right_vbox = QtWidgets.QVBoxLayout()
        right_vbox.addLayout(ctrl_btn_hbox)
        right_vbox.addLayout(grid)
        right_vbox.addWidget(self.log_monitor)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addLayout(left_vbox)
        hbox.addLayout(right_vbox)

        self.setLayout(hbox)


    def register_event(self) -> None:
        """"""
        self.signal_log.connect(self.process_log_event)
        self.event_engine.register(EVENT_OPTION_HEDGE_ALGO_LOG, self.signal_log.emit)

    def process_log_event(self, event: Event) -> None:
        """"""
        log = event.data
        timestr = log.time.strftime("%H:%M:%S")
        msg = f"{timestr}  {log.msg}"
        self.log_monitor.append(msg)

    def show(self) -> None:
        """"""
        self.hedge_engine.init_engine()
        self.hedge_monitor.resizeColumnsToContents()
        super().showMaximized()

    def start_for_all(self) -> None:
        for chain_symbol in self.hedge_monitor.cells.keys():
            self.hedge_monitor.start_auto_hedge(chain_symbol)

    def stop_for_all(self) -> None:
        self.hedge_engine.stop_all_auto_hedge()

    def set_offset_percent(self) -> None:
        offset_percent = self.offset_percent.get_display_value()

        for cells in self.hedge_monitor.cells.values():
            if cells['offset_percent'].isEnabled():
                cells['offset_percent'].setValue(offset_percent)

    def set_hedge_percent(self) -> None:
        hedge_percent = self.hedge_percent.get_display_value()

        for cells in self.hedge_monitor.cells.values():
            if cells['hedge_percent'].isEnabled():
                cells['hedge_percent'].setValue(hedge_percent)

    def close(self) -> None:
        self.hedge_engine.save_setting()
        self.hedge_engine.save_data()