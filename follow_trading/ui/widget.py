from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtWidgets, QtCore, QtGui
from vnpy.trader.ui.widget import (
    BaseCell,
    MsgCell,
    TimeCell,
    BidCell,
    AskCell,
    PnlCell,
    BaseMonitor
)

from vnpy.trader.constant import (
    OrderType
)

from ..engine import (
    APP_NAME,
    FollowEngine,
    EVENT_FOLLOW_LOG,
    EVENT_FOLLOW_POS_DELTA
)


class ComboBox(QtWidgets.QComboBox):
    pop_show = QtCore.pyqtSignal()

    def showPopup(self):
        self.pop_show.emit()
        super(ComboBox, self).showPopup()


class FollowManager(QtWidgets.QWidget):
    signal_log = QtCore.pyqtSignal(Event)
    # timer = QtCore.QTimer()

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(FollowManager, self).__init__()

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.follow_engine = main_engine.get_engine(APP_NAME)

        self.sync_symbol = ''

        self.init_ui()
        self.follow_engine.init_engine()
        self.register_event()

    def init_ui(self):
        """"""
        self.setWindowTitle("跟随交易")
        self.setMinimumSize(1280, 950)
        self.setMaximumSize(1920, 1080)

        # create widgets
        self.start_button = QtWidgets.QPushButton("启动")
        self.start_button.clicked.connect(self.start_follow)

        self.stop_button = QtWidgets.QPushButton("停止")
        self.stop_button.clicked.connect(self.stop_follow)
        self.stop_button.setEnabled(False)

        self.sync_open_button = QtWidgets.QPushButton("单合约开仓同步")
        self.sync_open_button.clicked.connect(self.sync_open)

        self.sync_close_button = QtWidgets.QPushButton("单合约平仓同步")
        self.sync_close_button.clicked.connect(self.sync_close)

        self.sync_button = QtWidgets.QPushButton("单合约开平同步")
        self.sync_button.clicked.connect(self.sync_open_and_close)

        self.sync_all_button = QtWidgets.QPushButton("所有持仓同步")
        self.sync_all_button.clicked.connect(self.sync_all)

        self.sync_net_button = QtWidgets.QPushButton("日内交易同步")
        self.sync_net_button.clicked.connect(lambda: self.sync_net_delta(is_sync_baisc=False))

        self.sync_basic_button = QtWidgets.QPushButton("日内底仓同步")
        self.sync_basic_button.clicked.connect(lambda: self.sync_net_delta(is_sync_baisc=True))

        self.modify_pos_button = QtWidgets.QPushButton("手动修改仓位")
        self.modify_pos_button.clicked.connect(self.manual_modify_pos)

        self.close_hedged_pos_button = QtWidgets.QPushButton("锁仓单平仓")
        self.close_hedged_pos_button.clicked.connect(self.close_hedged_pos)

        for btn in [self.start_button,
                    self.stop_button,
                    self.sync_open_button,
                    self.sync_close_button,
                    self.sync_button,
                    self.sync_all_button,
                    self.sync_net_button,
                    self.sync_basic_button,
                    self.modify_pos_button,
                    self.close_hedged_pos_button]:
            btn.setFixedHeight(btn.sizeHint().height() * 2)

        gateways = self.follow_engine.get_connected_gateway_names()

        self.source_combo = ComboBox()
        self.source_combo.addItems(gateways)
        self.source_combo.pop_show.connect(self.refresh_gateway_name)
        self.target_combo = ComboBox()
        self.target_combo.addItems(gateways)
        self.target_combo.pop_show.connect(self.refresh_gateway_name)

        self.order_type_combo = QtWidgets.QComboBox()
        self.order_type_combo.addItems(['限价', '市价'])
        self.order_type_combo.activated[str].connect(self.set_order_type)

        self.follow_direction_combo = QtWidgets.QComboBox()
        self.follow_direction_combo.addItems(['正向跟随', '反向跟随'])

        self.sync_symbol_combo = ComboBox()
        self.sync_symbol_combo.pop_show.connect(self.refresh_symbol_list)
        self.sync_symbol_combo.activated[str].connect(self.set_sync_symbol)

        validator = QtGui.QIntValidator()
        self.timeout_line = QtWidgets.QLineEdit(str(self.follow_engine.cancel_order_timeout))
        self.timeout_line.setValidator(validator)
        self.timeout_line.editingFinished.connect(self.set_cancel_order_timeout)
        self.tickout_line = QtWidgets.QLineEdit(str(self.follow_engine.tick_add))
        self.tickout_line.setValidator(validator)
        self.tickout_line.editingFinished.connect(self.set_tick_add)
        self.multiples_line = QtWidgets.QLineEdit(str(self.follow_engine.multiples))
        self.multiples_line.setValidator(validator)
        self.multiples_line.editingFinished.connect(self.set_multiples)
        self.single_max_line = QtWidgets.QLineEdit(str(self.follow_engine.single_max))
        self.single_max_line.setValidator(validator)
        self.single_max_line.editingFinished.connect(self.set_single_max)

        self.pos_delta_monitor = PosDeltaMonitor(self.main_engine, self.event_engine)
        self.log_monitor = LogMonitor(self.main_engine, self.event_engine)

        # set layout
        form = QtWidgets.QFormLayout()
        form.addRow("跟随接口名", self.source_combo)
        form.addRow("发单接口名", self.target_combo)
        form.addRow("发单类型", self.order_type_combo)
        form.addRow("跟单方向", self.follow_direction_combo)
        form.addRow("超时自动撤单（秒）", self.timeout_line)
        form.addRow("超价下单档位", self.tickout_line)
        form.addRow("跟随倍数", self.multiples_line)
        form.addRow("单笔最大手数", self.single_max_line)
        form.addRow(self.start_button)
        form.addRow(self.stop_button)

        form_sync = QtWidgets.QFormLayout()
        form_sync.addRow("同步合约", self.sync_symbol_combo)
        form_sync.addRow(self.sync_open_button)
        form_sync.addRow(self.sync_close_button)
        form_sync.addRow(self.sync_button)
        form_sync.addRow(self.sync_all_button)
        form_sync.addRow(self.sync_net_button)
        form_sync.addRow(self.sync_basic_button)
        form_sync.addRow(self.modify_pos_button)
        form_sync.addRow(self.close_hedged_pos_button)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(form)
        vbox.addStretch()
        vbox.addLayout(form_sync)
        vbox.addStretch()

        grid = QtWidgets.QGridLayout()
        grid.addLayout(vbox, 0, 0, 2, 1)
        grid.addWidget(self.pos_delta_monitor, 0, 1)
        grid.addWidget(self.log_monitor, 1, 1)
        grid.setColumnStretch(0, 1)
        grid.setColumnStretch(1, 3)

        self.setLayout(grid)

    def register_event(self):
        """"""
        # self.timer.start(3000)
        # self.timer.timeout.connect(self.refresh_symbol_list)
        # self.timer.timeout.connect(self.test_timer2)
        # self.signal_log.connect(self.process_log_event)
        # self.event_engine.register(EVENT_FOLLOW_LOG, self.signal_log.emit)
        pass

    def set_sync_symbol(self, vt_symbol: str):
        """
        Set symbol to be synced
        """
        self.sync_symbol = vt_symbol
        self.write_log(f"选中合约{self.sync_symbol}")

    def set_order_type(self, order_type: str):
        """"""
        if order_type == "限价":
            self.follow_engine.set_parameters('order_type', OrderType.LIMIT)
        else:
            self.follow_engine.set_parameters('order_type', OrderType.MARKET)
        self.write_log(f"发单类型：{self.follow_engine.order_type.value} 切换成功")

    def set_cancel_order_timeout(self):
        """"""
        text = self.timeout_line.text()
        self.follow_engine.set_parameters('cancel_order_timeout', int(text))
        self.write_log(f"未成交自动撤单超时：{self.follow_engine.cancel_order_timeout} 秒设置成功")

    def set_tick_add(self):
        """"""
        text = self.tickout_line.text()
        self.follow_engine.set_parameters('tick_add', int(text))
        self.write_log(f"超价档位：{self.follow_engine.tick_add} 设置成功")

    def set_multiples(self):
        """"""
        text = self.multiples_line.text()
        self.follow_engine.set_parameters('multiples', int(text))
        self.write_log(f"跟随倍数：{self.follow_engine.multiples} 设置成功")

    def set_single_max(self):
        """"""
        text = self.single_max_line.text()
        self.follow_engine.set_parameters('single_max', int(text))
        self.write_log(f"单笔最大手数：{self.follow_engine.single_max} 设置成功")

    def refresh_gateway_name(self):
        """"""
        gateways = self.follow_engine.get_connected_gateway_names()
        for combo in [self.source_combo, self.target_combo]:
            combo.clear()
            combo.addItems(gateways)
        self.write_log(f"接口名称获取成功")

    def refresh_symbol_list(self):
        """"""
        self.sync_symbol_combo.clear()
        symbol_list = list(self.follow_engine.get_positions().keys())
        self.sync_symbol_combo.addItems(symbol_list)

    def test_timer(self):
        """"""
        self.write_log("定时器测试")

    def test_timer2(self):
        """"""
        self.write_log("定时器多槽测试")

    def start_follow(self):
        """"""
        source = self.source_combo.currentText()
        target = self.target_combo.currentText()
        if source == target:
            self.follow_engine.write_log("跟随接口和发单接口不能是同一个")
            return
        self.follow_engine.set_gateways(source, target)

        follow_direction = self.follow_direction_combo.currentText()
        if follow_direction == '正向跟随':
            is_inverse = False
        else:
            is_inverse = True
        self.follow_engine.set_parameters('inverse_follow', is_inverse)

        order_type = self.order_type_combo.currentText()
        self.follow_engine.set_parameters('order_type', OrderType(order_type))

        self.follow_engine.set_parameters('multiples', int(self.multiples_line.text()))
        self.follow_engine.set_parameters('tick_add', int(self.tickout_line.text()))
        # self.follow_engine.set_parameters('filter_trade_timeout', int(self.timeout_line.text()))

        result = self.follow_engine.start()
        if result:
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(True)

            self.source_combo.setEnabled(False)
            self.target_combo.setEnabled(False)
            self.follow_direction_combo.setEnabled(False)

    def stop_follow(self):
        """"""
        result = self.follow_engine.stop()
        if result:
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(False)

    def validate_vt_symbol(self, vt_symbol: str):
        """"""
        if not vt_symbol:
            self.write_log(f"未选择要同步的合约")
            return
        vt_symbol = vt_symbol.strip()
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f"尚未连接交易接口或合约{vt_symbol}不是可交易的合约")
        else:
            return vt_symbol

    def sync_open(self):
        """"""
        vt_symbol = self.sync_symbol
        if self.validate_vt_symbol(vt_symbol):
            self.follow_engine.sync_open_pos(vt_symbol)

    def sync_close(self):
        """"""
        vt_symbol = self.sync_symbol
        if self.validate_vt_symbol(vt_symbol):
            self.follow_engine.sync_close_pos(vt_symbol)

    def sync_open_and_close(self):
        """"""
        vt_symbol = self.sync_symbol
        if self.validate_vt_symbol(vt_symbol):
            self.follow_engine.sync_pos(vt_symbol)

    def sync_all(self):
        """"""
        self.follow_engine.sync_all_pos()

    def sync_net_delta(self, is_sync_baisc: bool):
        vt_symbol = self.sync_symbol
        if self.validate_vt_symbol(vt_symbol):
            self.follow_engine.sync_net_pos_delta(vt_symbol, is_sync_baisc)

    def manual_modify_pos(self):
        dialog = PosEditor(self.follow_engine)
        dialog.exec_()

    def close_hedged_pos(self):
        dialog = CloseHedgedDialog(self.follow_engine)
        dialog.exec_()

    def write_log(self, msg: str):
        """"""
        self.follow_engine.write_log(msg)

    def clear_log(self):
        """"""
        self.log_monitor.setRowCount(0)

    def show(self):
        """"""
        self.showNormal()


class PosDeltaMonitor(BaseMonitor):
    """
    Monitor for position delta.
    """
    event_type = EVENT_FOLLOW_POS_DELTA
    data_key = "vt_symbol"
    sorting = True

    headers = {
        "vt_symbol": {"display": "合约代码", "cell": BaseCell, "update": False},
        "source_long": {"display": "源账户多仓", "cell": BidCell, "update": True},
        "source_short": {"display": "源账户空仓", "cell": AskCell, "update": True},
        "source_net": {"display": "源账户净仓", "cell": PnlCell, "update": True},
        "target_long": {"display": "目标户多仓", "cell": BidCell, "update": True},
        "target_short": {"display": "目标户空仓", "cell": AskCell, "update": True},
        "target_net": {"display": "目标户净仓", "cell": PnlCell, "update": True},
        "long_delta": {"display": "多头仓差", "cell": BaseCell, "update": True},
        "short_delta": {"display": "空头仓差", "cell": BaseCell, "update": True},
        "net_delta": {"display": "净仓差", "cell": PnlCell, "update": True},
        "basic_delta": {"display": "底仓差", "cell": PnlCell, "update": True}
    }

    def init_ui(self):
        super(PosDeltaMonitor, self).init_ui()
        self.resize_columns()

class LogMonitor(BaseMonitor):
    """
    Monitor for log data.
    """
    event_type = EVENT_FOLLOW_LOG
    data_key = ""
    sorting = False

    headers = {
        "time": {"display": "时间", "cell": TimeCell, "update": False},
        "msg": {"display": "信息", "cell": MsgCell, "update": False},
    }

    def init_ui(self):
        super(LogMonitor, self).init_ui()
        self.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.Stretch)

    def insert_new_row(self, data):
        super(LogMonitor, self).insert_new_row(data)
        self.resizeRowToContents(0)


class PosEditor(QtWidgets.QDialog):
    def __init__(self, follow_engine: FollowEngine):
        super().__init__()

        self.follow_engine = follow_engine
        self.modify_symbol = ""

        self.init_ui()


    def init_ui(self):
        self.setWindowTitle("手动修改仓位")

        self.symbol_combo = ComboBox()
        self.symbol_combo.pop_show.connect(self.refresh_symbol_list)
        self.symbol_combo.activated[str].connect(self.set_modify_symbol)

        validator = QtGui.QIntValidator()

        self.long_pos_line = QtWidgets.QLineEdit()
        self.long_pos_line.setValidator(validator)

        self.short_pos_line = QtWidgets.QLineEdit()
        self.short_pos_line.setValidator(validator)

        self.basic_delta_line = QtWidgets.QLineEdit()
        self.basic_delta_line.setValidator(validator)

        button_modify = QtWidgets.QPushButton("修改")
        button_modify.clicked.connect(self.modify)

        form = QtWidgets.QFormLayout()
        form.addRow("合约代码", self.symbol_combo)
        form.addRow("目标户多仓", self.long_pos_line)
        form.addRow("目标户空仓", self.short_pos_line)
        form.addRow("底仓差", self.basic_delta_line)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(button_modify)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(form)
        vbox.addLayout(hbox)

        self.setLayout(vbox)


    def set_modify_symbol(self, vt_symbol: str):
        """
        Set symbol to be modified
        """
        self.modify_symbol = vt_symbol
        target_long = self.follow_engine.get_pos(vt_symbol, 'target_long')
        target_short = self.follow_engine.get_pos(vt_symbol, 'target_short')
        basic_delta = self.follow_engine.get_pos(vt_symbol, 'basic_delta')

        self.basic_delta_line.setText(str(basic_delta))
        self.long_pos_line.setText(str(target_long))
        self.short_pos_line.setText(str(target_short))
        self.write_log(f"选中合约{self.modify_symbol}")

    def refresh_symbol_list(self):
        """"""
        self.symbol_combo.clear()
        symbol_list = list(self.follow_engine.get_positions().keys())
        self.symbol_combo.addItems(symbol_list)

    def modify(self):
        """"""
        new_basic_delta = self.basic_delta_line.text()
        new_long = self.long_pos_line.text()
        new_short = self.short_pos_line.text()
        self.follow_engine.set_pos(self.modify_symbol, 'basic_delta', int(new_basic_delta))
        self.follow_engine.set_pos(self.modify_symbol, 'target_long', int(new_long))
        self.follow_engine.set_pos(self.modify_symbol, 'target_short', int(new_short))
        self.follow_engine.save_follow_data()
        self.write_log(f"{self.modify_symbol}仓位修改成功")

    def write_log(self, msg: str):
        """"""
        self.follow_engine.write_log(msg)



class CloseHedgedDialog(QtWidgets.QDialog):
    def __init__(self, follow_engine: FollowEngine):
        super().__init__()

        self.follow_engine = follow_engine
        self.close_symbol = ""

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("已对冲仓位平仓")

        self.symbol_combo = ComboBox()
        self.symbol_combo.pop_show.connect(self.refresh_symbol_list)
        self.symbol_combo.activated[str].connect(self.set_close_symbol)

        validator = QtGui.QIntValidator()
        self.close_pos_line = QtWidgets.QLineEdit()
        self.close_pos_line.setValidator(validator)

        button_close = QtWidgets.QPushButton("平仓")
        button_close.clicked.connect(self.close_hedged_pos)

        form = QtWidgets.QFormLayout()
        form.addRow("合约代码", self.symbol_combo)
        form.addRow("平仓手数", self.close_pos_line)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(button_close)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(form)
        vbox.addLayout(hbox)

        self.setLayout(vbox)


    def set_close_symbol(self, vt_symbol: str):
        """
        Set symbol to be clsoed
        """
        self.modify_symbol = vt_symbol
        target_long = self.follow_engine.get_pos(vt_symbol, 'target_long')
        target_short = self.follow_engine.get_pos(vt_symbol, 'target_short')
        avaiable = min(target_long, target_short)

        self.close_pos_line.setText(str(avaiable))
        self.write_log(f"选中合约{self.modify_symbol}")

    def refresh_symbol_list(self):
        """"""
        self.symbol_combo.clear()
        symbol_list = list(self.follow_engine.get_positions().keys())
        self.symbol_combo.addItems(symbol_list)

    def close_hedged_pos(self):
        """"""
        pos = self.close_pos_line.text()
        self.follow_engine.close_hedged_pos(self.modify_symbol, int(pos))

    def write_log(self, msg: str):
        """"""
        self.follow_engine.write_log(msg)