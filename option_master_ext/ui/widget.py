from typing import List, Dict

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtWidgets, QtGui

from vnpy.app.option_master.ui.widget import PortfolioDialog

from ..engine_ext import (
    APP_NAME, 
    OptionEngineExt
)
from .manager import HedgeManager

class PortfolioDialogExt(PortfolioDialog):
    def __init__(self, option_engine: OptionEngineExt, portfolio_name: str):
        super().__init__(option_engine, portfolio_name)

    def update_portfolio_setting(self) -> None:
        """"""
        model_name = self.model_name_combo.currentText()
        interest_rate = self.interest_rate_spin.value() / 100

        if self.inverse_combo.currentIndex() == 0:
            inverse = False
        else:
            inverse = True

        precision = self.precision_spin.value()

        chain_underlying_map = {}
        for chain_symbol, combo in self.combos.items():
            underlying_symbol = combo.currentText()

            if underlying_symbol:
                chain_underlying_map[chain_symbol] = underlying_symbol

        self.option_engine.update_portfolio_setting(
            self.portfolio_name,
            model_name,
            interest_rate,
            chain_underlying_map,
            inverse,
            precision
        )

        self.close()


class OptionManagerExt(QtWidgets.QWidget):

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__()

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.option_engine = main_engine.get_engine(APP_NAME)

        self.hedge_manager: HedgeManager = None
        self.volatility_trading = None

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("OptionMasterExt")

        self.portfolio_combo = QtWidgets.QComboBox()
        self.portfolio_combo.setFixedWidth(150)
        self.update_portfolio_combo()

        self.portfolio_button = QtWidgets.QPushButton("配置")
        self.portfolio_button.clicked.connect(self.open_portfolio_dialog)

        self.init_button = QtWidgets.QPushButton("初始化")
        self.init_button.clicked.connect(self.init_engine)

        self.volatility_button = QtWidgets.QPushButton("波动率交易")
        self.hedge_button = QtWidgets.QPushButton("Delta对冲")

        for button in [
            self.volatility_button,
            self.hedge_button,
        ]:
            button.setEnabled(False)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(self.portfolio_combo)
        hbox.addWidget(self.portfolio_button)
        hbox.addWidget(self.init_button)
        hbox.addWidget(self.volatility_button)
        hbox.addWidget(self.hedge_button)

        self.setLayout(hbox)

    def update_portfolio_combo(self) -> None:
        """"""
        if not self.portfolio_combo.isEnabled():
            return

        self.portfolio_combo.clear()
        portfolio_names = self.option_engine.get_portfolio_names()
        self.portfolio_combo.addItems(portfolio_names)


    def open_portfolio_dialog(self) -> None:
        """"""
        portfolio_name = self.portfolio_combo.currentText()
        if not portfolio_name:
            return

        self.portfolio_name = portfolio_name

        dialog = PortfolioDialogExt(self.option_engine, portfolio_name)
        dialog.exec_()

    def init_engine(self) -> None:
        self.option_engine.init_engine()

        self.portfolio_combo.setEnabled(False)
        self.portfolio_button.setEnabled(False)
        self.init_button.setEnabled(False)
        
        self.init_widgets()

    def init_widgets(self) -> None:
        self.hedge_manager = HedgeManager(self.option_engine)
        self.hedge_button.clicked.connect(self.hedge_manager.show)

        self.hedge_button.setEnabled(True)

    def register_event(self) -> None:
        pass


    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """"""
        if self.hedge_manager:
            self.hedge_manager.close()

        event.accept()