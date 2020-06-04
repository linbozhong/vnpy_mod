from typing import List, Dict

from vnpy.event import EventEngine, Event
from vnpy.trader.ui import QtWidgets, QtCore, QtGui
from vnpy.trader.ui.widget import BaseCell, DirectionCell, EnumCell, BaseMonitor

from vnpy.app.option_master.base import ChainData
from vnpy.app.option_master.ui.manager import AlgoSpinBox, AlgoDoubleSpinBox
from vnpy.app.option_master.ui.monitor import MonitorCell, PosCell, GreeksCell


from ..engine_ext import (
    EVENT_OPTION_STRATEGY_ORDER, EVENT_OPTION_HEDGE_ALGO_STATUS,
    OptionEngineExt, HedgeEngine, ChannelHedgeAlgo
)


class HedgeAutoButton(QtWidgets.QPushButton):
    def __init__(self, chain_symbol: str, monitor: "HedgeMonitor"):
        super().__init__()
        self.chain_symbol = chain_symbol
        self.monitor = monitor

        self.active = False
        self.setText("OFF")
        self.clicked.connect(self.on_clicked)

    def on_clicked(self) -> None:
        if self.active:
            self.monitor.stop_auto_hedge(self.chain_symbol)
        else:
            self.monitor.start_auto_hedge(self.chain_symbol)

    def update_status(self, active: bool) -> None:
        self.active = active

        if active:
            self.setText("ON")
        else:
            self.setText("OFF")


class HedgeActionButton(QtWidgets.QPushButton):
    def __init__(self, chain_symbol: str, monitor: "HedgeMonitor"):
        super().__init__()
        self.chain_symbol = chain_symbol
        self.monitor = monitor

        self.setText("执行")
        self.clicked.connect(self.on_clicked)

    def on_clicked(self) -> None:
        algo = self.monitor.hedge_engine.hedge_algos.get(self.chain_symbol)
        if algo:
            if algo.is_hedging():
                return
            else:
                pass


class HedgePercentSpinBox(AlgoSpinBox):
    def __init__(self):
        super().__init__()
        self.setMaximum(100)
        self.setMinimum(10)
        self.setSingleStep(10)

    def get_real_value(self) -> float:
        return self.value() / 100

    def get_display_value(self) -> int:
        return self.value()

    def update_status(self, active: bool) -> None:
        self.setEnabled(not active)


class OffsetPercentSpinBox(AlgoDoubleSpinBox):
    def __init__(self):
        super().__init__()
        self.setMaximum(10)
        self.setMinimum(0.2)
        self.setSingleStep(0.2)

    def get_real_value(self) -> float:
        return self.value() / 100

    def get_display_value(self) -> float:
        return self.value()

    def update_status(self, active: bool) -> None:
        self.setEnabled(not active)


class StrategyOrderMonitor(BaseMonitor):
    event_type = EVENT_OPTION_STRATEGY_ORDER
    data_key = "strategy_id"
    sorting = True

    headers = {
        "strategy_id": {"display": "策略ID", "cell": BaseCell, "update": False},
        "chain_symbol": {"display": "期权链", "cell": BaseCell, "update": False},
        "time": {"display": "时间", "cell": BaseCell, "update": False},
        "strategy_name": {"display": "策略名", "cell": EnumCell, "update": False},
        "direction": {"display": "方向", "cell": DirectionCell, "update": False},
        "status": {"display": "状态", "cell": EnumCell, "update": True},
    }

    def init_ui(self):
        super().init_ui()
        self.resize_columns()


class HedgeMonitor(QtWidgets.QTableWidget):
    signal_status = QtCore.pyqtSignal(Event)

    headers: List[Dict] = [
        {"name": "chain_symbol", "display": "期权链", "cell": MonitorCell},
        {"name": "balance_price", "display": "中性基准价", "cell": MonitorCell},
        {"name": "up_price", "display": "上阈值", "cell": MonitorCell},
        {"name": "down_price", "display": "下阈值", "cell": MonitorCell},
        {"name": "pos_delta", "display": "Delta", "cell": GreeksCell},
        {"name": "net_pos", "display": "组合净仓", "cell": PosCell},
        {"name": "offset_percent", "display": "偏移比例", "cell": OffsetPercentSpinBox},
        {"name": "hedge_percent", "display": "对冲比例", "cell": HedgePercentSpinBox},
        {"name": "status", "display": "状态", "cell": MonitorCell},
        {"name": "auto_hedge", "display": "监测开关", "cell": HedgeAutoButton},
        {"name": "action_hedge", "display": "对冲", "cell": HedgeActionButton},
    ]

    def __init__(self, option_engine: OptionEngineExt):
        super().__init__()

        self.option_engine: OptionEngineExt = option_engine
        self.event_engine: EventEngine = option_engine.event_engine
        self.hedge_engine: HedgeEngine = self.option_engine.hedge_engine
        
        self.chains: Dict[str, ChainData] = self.hedge_engine.chains
        self.cells: Dict[str, Dict] = {}

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        self.setWindowTitle("通道对冲")
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(self.NoEditTriggers)

        chain_symbols = self.chains.keys()
        self.setRowCount(len(chain_symbols))
        self.setColumnCount(len(self.headers))

        labels = [d["display"] for d in self.headers]
        self.setHorizontalHeaderLabels(labels)

        for row, chain_symbol in enumerate(chain_symbols):
            chain_cells = {}
            for column, d in enumerate(self.headers):
                cell_type  = d['cell']
                cell_name = d['name']

                if cell_name in ['auto_hedge', 'action_hedge']:
                    cell = cell_type(chain_symbol, self)
                else:
                    cell = cell_type()

                if isinstance(cell, QtWidgets.QTableWidgetItem):
                    self.setItem(row, column, cell)
                else:
                    self.setCellWidget(row, column, cell)

                chain_cells[cell_name] = cell
            
            self.cells[chain_symbol] = chain_cells

        self.resizeColumnsToContents()

        for chain_symbol in self.cells:
            algo = self.hedge_engine.hedge_algos.get(chain_symbol)
            self.update_algo_status(algo)
            self.update_chain_attr(chain_symbol, 'chain_symbol')
            self.update_chain_attr(chain_symbol, 'net_pos')
            self.update_chain_attr(chain_symbol, 'pos_delta')

    def register_event(self) -> None:
        self.signal_status.connect(self.process_status_event)
        self.event_engine.register(EVENT_OPTION_HEDGE_ALGO_STATUS, self.signal_status.emit)

    def process_status_event(self, event: Event) -> None:
        algo = event.data
        self.update_algo_status(algo)
        
    def update_algo_status(self, algo: ChannelHedgeAlgo):
        cells = self.cells[algo.chain_symbol]

        cells['status'].setText(algo.status.value)
        cells['balance_price'].setText(f'{algo.balance_price:0.3f}')
        cells['up_price'].setText(f'{algo.up_price:0.3f}')
        cells['down_price'].setText(f'{algo.down_price:0.3f}')

        print('update algo status:', algo.chain.net_pos, algo.chain.pos_delta)
        cells['net_pos'].setText(str(algo.chain.net_pos))
        cells['pos_delta'].setText(f'{algo.chain.pos_delta:0.0f}')
        cells['auto_hedge'].update_status(algo.is_active())

        cells['offset_percent'].setValue(algo.offset_percent * 100)
        cells['hedge_percent'].setValue(int(algo.hedge_percent * 100))
        cells['offset_percent'].update_status(algo.is_active())
        cells['hedge_percent'].update_status(algo.is_active())

    def update_chain_attr(self, chain_symbol: str, attr_name: str):
        chain = self.chains.get(chain_symbol)
        cells = self.cells[chain_symbol]

        if attr_name in cells:
            value = getattr(chain, attr_name, None)
            if value is not None:
                cells[attr_name].setText(str(value))
    
    def start_auto_hedge(self, chain_symbol) -> None:
        cells = self.cells[chain_symbol]
        params = {}
        for name in ['offset_percent', 'hedge_percent']:
            params[name] = cells[name].get_real_value()

        self.hedge_engine.start_hedge_algo(chain_symbol, params)

    def stop_auto_hedge(self, chain_symbol) -> None:
        self.hedge_engine.stop_hedge_algo(chain_symbol)



