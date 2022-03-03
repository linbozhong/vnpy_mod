from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtWidgets, QtCore, QtGui
from vnpy.trader.utility import TRADER_DIR
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
    EVENT_FOLLOW_POS_DELTA,
    EVENT_FOLLOW_MODIFY_POS,
    OrderBasePrice,
    FollowBaseMode
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

        self.is_gateway_inited = False

        self.init_ui()
        self.follow_engine.init_engine()
        self.register_event()

    def init_ui(self):
        """"""
        self.setWindowTitle(f"跟随交易 [{TRADER_DIR}]")
        self.setMinimumSize(920, 750)
        self.setMaximumSize(1920, 1080)

        # create widgets
        self.start_button = QtWidgets.QPushButton("启动")
        self.start_button.clicked.connect(self.start_follow)

        self.stop_button = QtWidgets.QPushButton("停止")
        self.stop_button.clicked.connect(self.stop_follow)
        self.stop_button.setEnabled(False)

        self.sync_pos_button = QtWidgets.QPushButton("同步持仓")
        self.sync_pos_button.clicked.connect(self.sync_pos)
        self.sync_pos_button.setEnabled(False)

        self.modify_pos_button = QtWidgets.QPushButton("修改仓位")
        self.modify_pos_button.clicked.connect(self.manual_modify_pos)

        self.set_skip_button = QtWidgets.QPushButton("同步设置")
        self.set_skip_button.clicked.connect(self.set_skip_contracts)

        self.set_order_button = QtWidgets.QPushButton("委托设置")
        self.set_order_button.clicked.connect(self.set_order_setting)

        self.close_hedged_pos_button = QtWidgets.QPushButton("锁仓单平仓")
        self.close_hedged_pos_button.clicked.connect(self.close_hedged_pos)
        self.close_hedged_pos_button.setEnabled(False)

        for btn in [self.start_button,
                    self.stop_button,
                    self.sync_pos_button,
                    self.modify_pos_button,
                    self.set_skip_button,
                    self.set_order_button,
                    self.close_hedged_pos_button]:
            btn.setFixedHeight(btn.sizeHint().height() * 2)

        gateways = self.follow_engine.get_connected_gateway_names()
        if len(gateways) == 2:
            self.is_gateway_inited = True

        self.source_combo = ComboBox()
        self.source_combo.addItems(gateways)
        self.source_combo.pop_show.connect(self.refresh_gateway_name)
        self.target_combo = ComboBox()
        self.target_combo.addItems(gateways)
        self.target_combo.pop_show.connect(self.refresh_gateway_name)

        self.skip_contracts_combo = ComboBox()
        self.skip_contracts_combo.pop_show.connect(self.refresh_skip_contracts)
        self.refresh_skip_contracts()

        self.intraday_combo = ComboBox()
        self.intraday_combo.pop_show.connect(self.refresh_intraday)
        self.refresh_intraday()

        self.order_vol_combo = ComboBox()
        self.order_vol_combo.pop_show.connect(self.refresh_order_vols)
        self.refresh_order_vols()

        self.follow_direction_combo = QtWidgets.QComboBox()
        self.follow_direction_combo.addItems(['正向跟随', '反向跟随'])
        self.follow_direction_combo.activated[str].connect(self.set_follow_direction)
        self.get_current_follow_direction()
        self.follow_direction_combo.setEnabled(False)

        self.intraday_trading_combo = QtWidgets.QComboBox()
        self.intraday_trading_combo.addItems(['是', '否'])
        self.intraday_trading_combo.activated[str].connect(self.set_is_intraday_trading)
        self.get_current_intraday_trading()


        self.follow_based_combo = QtWidgets.QComboBox()
        self.follow_based_combo.addItems(['跟随委托', '跟随成交'])
        self.follow_based_combo.activated[str].connect(self.set_follow_based)
        self.get_current_follow_based()

        validator = QtGui.QIntValidator()

        self.follow_timeout_line = QtWidgets.QLineEdit(str(self.follow_engine.filter_trade_timeout))
        self.follow_timeout_line.setValidator(validator)
        self.follow_timeout_line.editingFinished.connect(self.set_follow_timeout)

        self.multiples_line = QtWidgets.QLineEdit(str(self.follow_engine.multiples))
        self.multiples_line.setValidator(validator)
        self.multiples_line.editingFinished.connect(self.set_multiples)

        self.pos_delta_monitor = PosDeltaMonitor(self.main_engine, self.event_engine)
        self.log_monitor = LogMonitor(self.main_engine, self.event_engine)

        # Set layout
        form = QtWidgets.QFormLayout()
        form.addRow("标准户接口", self.source_combo)
        form.addRow("跟单户接口", self.target_combo)
        form.addRow("跟单方向", self.follow_direction_combo)
        form.addRow("超时禁跟（秒）", self.follow_timeout_line)
        form.addRow("跟随倍数", self.multiples_line)
        form.addRow("是否日内交易", self.intraday_trading_combo)
        form.addRow("信号来源", self.follow_based_combo)
        form.addRow(self.start_button)
        form.addRow(self.stop_button)

        form_action = QtWidgets.QFormLayout()
        form_action.addRow("日内模式品种", self.intraday_combo)
        form_action.addRow("禁止同步合约", self.skip_contracts_combo)
        form_action.addRow("跟单委托手数", self.order_vol_combo)
        form_action.addRow(self.modify_pos_button)
        form_action.addRow(self.set_skip_button)
        form_action.addRow(self.set_order_button)
        form_action.addRow(self.sync_pos_button)
        form_action.addRow(self.close_hedged_pos_button)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(form)
        vbox.addStretch()
        vbox.addLayout(form_action)
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

    def set_follow_direction(self, follow_direction: str):
        """"""
        if follow_direction == "正向跟随":
            self.follow_engine.set_parameters('inverse_follow', False)
        else:
            self.follow_engine.set_parameters('inverse_follow', True)
        self.write_log(f"是否反向跟单：{self.follow_engine.inverse_follow} 设置成功")

    def set_follow_based(self, follow_based: str):
        if follow_based == "跟随委托":
            self.follow_engine.set_parameters('follow_based', FollowBaseMode.BASE_ORDER)
        else:
            self.follow_engine.set_parameters('follow_based', FollowBaseMode.BASE_TRADE)
        self.write_log(f"跟单信号模式：{self.follow_engine.follow_based.value} 设置成功")


    def set_is_intraday_trading(self, intraday_flag: str):
        """"""
        if intraday_flag == "是":
            self.follow_engine.set_parameters('is_intraday_trading', True)
        else:
            self.follow_engine.set_parameters('is_intraday_trading', False)
        self.write_log(f"是否日内交易：{self.follow_engine.is_intraday_trading}")

    def get_current_follow_direction(self):
        """"""
        inverse_follow = self.follow_engine.inverse_follow
        if not inverse_follow:
            self.follow_direction_combo.setCurrentIndex(0)
        else:
            self.follow_direction_combo.setCurrentIndex(1)

    def get_current_intraday_trading(self):
        """"""
        is_intraday_trading = self.follow_engine.is_intraday_trading
        if is_intraday_trading:
            self.intraday_trading_combo.setCurrentIndex(0)
        else:
            self.intraday_trading_combo.setCurrentIndex(1)

    def get_current_follow_based(self):
        follow_based = self.follow_engine.follow_based
        if follow_based == FollowBaseMode.BASE_ORDER:
            self.follow_based_combo.setCurrentIndex(0)
        else:
            self.follow_based_combo.setCurrentIndex(1)

    def set_follow_timeout(self):
        text = self.follow_timeout_line.text()
        self.follow_engine.set_parameters('filter_trade_timeout', int(text))
        self.write_log(f"成交单超时：{self.follow_engine.filter_trade_timeout} 秒设置成功")

    def set_multiples(self):
        """"""
        text = self.multiples_line.text()
        self.follow_engine.set_parameters('multiples', int(text))
        self.write_log(f"跟随倍数：{self.follow_engine.multiples} 设置成功")

    def refresh_gateway_name(self):
        """"""
        gateways = self.follow_engine.get_connected_gateway_names()
        if not gateways:
            self.write_log(f"获取不到可用接口名称，请先连接接口")
        else:
            for combo in [self.source_combo, self.target_combo]:
                combo.clear()
                combo.addItems(gateways)
            self.write_log(f"接口名称获取成功")

    def refresh_skip_contracts(self):
        """"""
        self.skip_contracts_combo.clear()
        symbol_list = self.follow_engine.get_skip_contracts()
        self.skip_contracts_combo.addItems(symbol_list)

    def refresh_intraday(self):
        """"""
        self.intraday_combo.clear()
        symbol_list = self.follow_engine.get_intraday_symbols()
        self.intraday_combo.addItems(symbol_list)

    def refresh_order_vols(self):
        self.order_vol_combo.clear()
        vol_list = self.follow_engine.get_order_vols_to_follow()
        vol_str_list = [str(vol) for vol in vol_list]
        self.order_vol_combo.addItems(vol_str_list)

    def test_timer(self):
        """"""
        self.write_log("定时器测试")

    def test_timer2(self):
        """"""
        self.write_log("定时器多槽测试")

    def start_follow(self):
        """"""
        if not self.is_gateway_inited:
            self.write_log("标准户接口和跟单户接口未全部初始化，请检查RPC是否已连接服务器，然后重启程序重试。")
            self.start_button.setEnabled(False)
            return

        self.pos_delta_monitor.resize_columns()

        source = self.source_combo.currentText()
        target = self.target_combo.currentText()
        if source == target:
            self.follow_engine.write_log("标准户接口和跟单户接口不能是同一个")
            return
        self.follow_engine.set_gateways(source, target)

        result = self.follow_engine.start()
        if result:
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(True)

            self.sync_pos_button.setEnabled(True)
            self.close_hedged_pos_button.setEnabled(True)
            # self.modify_pos_button.setEnabled(False)
            self.set_skip_button.setEnabled(True)

            self.source_combo.setEnabled(False)
            self.target_combo.setEnabled(False)
            self.follow_direction_combo.setEnabled(False)

    def stop_follow(self):
        """"""
        result = self.follow_engine.stop()
        if result:
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(False)
            self.sync_pos_button.setEnabled(False)
            self.close_hedged_pos_button.setEnabled(False)
            # self.modify_pos_button.setEnabled(True)

    def validate_vt_symbol(self, vt_symbol: str):
        """"""
        if not vt_symbol:
            self.write_log(f"合约名称不能为空，请正确选择或输入")
            return
        vt_symbol = vt_symbol.strip()
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f"{vt_symbol}无法匹配接口的可交易的合约，请检查合约是否正确或接口是否连接")
        else:
            return vt_symbol

    def sync_pos(self):
        dialog = SyncPosEditor(self, self.follow_engine)
        dialog.exec_()

    def manual_modify_pos(self):
        dialog = PosEditor(self, self.follow_engine)
        dialog.exec_()

    def set_skip_contracts(self):
        dialog = SkipContractEditor(self, self.follow_engine)
        dialog.exec_()

    def set_order_setting(self):
        dialog = OrderSettingEditor(self, self.follow_engine)
        dialog.exec_()

    def close_hedged_pos(self):
        dialog = CloseHedgedDialog(self, self.follow_engine)
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
        "source_long": {"display": "A多", "cell": BidCell, "update": True},
        "source_short": {"display": "A空", "cell": AskCell, "update": True},
        "source_net": {"display": "A净", "cell": PnlCell, "update": True},
        "target_long": {"display": "B多", "cell": BidCell, "update": True},
        "target_short": {"display": "B空", "cell": AskCell, "update": True},
        "target_net": {"display": "B净", "cell": PnlCell, "update": True},
        "long_delta": {"display": "多差", "cell": BaseCell, "update": True},
        "short_delta": {"display": "空差", "cell": BaseCell, "update": True},
        "net_delta": {"display": "净差", "cell": PnlCell, "update": True},
        "basic_delta": {"display": "底仓差", "cell": PnlCell, "update": True},
        "source_traded_net": {"display": "交易净", "cell": PnlCell, "update": True},
        'lost_follow_net': {"display": "丢失", "cell": PnlCell, "update": True}
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


class SyncPosEditor(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, follow_engine: FollowEngine):
        super().__init__()

        self.parent = parent
        self.follow_engine = follow_engine

        self.sync_symbol = ''

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("同步仓位")
        self.setMinimumWidth(300)

        # Select symbol widget
        self.sync_symbol_combo = ComboBox()
        self.sync_symbol_combo.pop_show.connect(self.refresh_symbol_list)
        self.sync_symbol_combo.activated[str].connect(self.set_sync_symbol)

        # Sync action button
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

        for btn in [self.sync_open_button,
                    self.sync_close_button,
                    self.sync_button,
                    self.sync_all_button,
                    self.sync_net_button,
                    self.sync_basic_button]:
            btn.setFixedHeight(btn.sizeHint().height() * 1.5)

        # Set layout
        form_sync = QtWidgets.QFormLayout()
        form_sync.addRow("同步合约", self.sync_symbol_combo)
        form_sync.addRow(self.sync_open_button)
        form_sync.addRow(self.sync_close_button)
        form_sync.addRow(self.sync_button)
        form_sync.addRow(self.sync_all_button)
        form_sync.addRow(self.sync_net_button)
        form_sync.addRow(self.sync_basic_button)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(form_sync)

        self.setLayout(vbox)

    def refresh_symbol_list(self):
        """"""
        self.sync_symbol_combo.clear()
        symbol_list = list(self.follow_engine.get_positions().keys())
        for contract in self.follow_engine.skip_contracts:
            if contract in symbol_list:
                symbol_list.remove(contract)
        self.sync_symbol_combo.addItems(symbol_list)

    def set_sync_symbol(self, vt_symbol: str):
        """"""
        self.sync_symbol = vt_symbol
        self.write_log(f"选中合约{self.sync_symbol}")

    def validate_vt_symbol(self, vt_symbol: str):
        """"""
        return self.parent.validate_vt_symbol(vt_symbol)

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
        """"""
        vt_symbol = self.sync_symbol
        if self.validate_vt_symbol(vt_symbol):
            self.follow_engine.sync_net_pos_delta(vt_symbol, is_sync_baisc)

    def write_log(self, msg: str):
        """"""
        self.follow_engine.write_log(msg)


class PosEditor(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, follow_engine: FollowEngine):
        super().__init__()

        self.parent = parent
        self.follow_engine = follow_engine
        self.modify_symbol = ""

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("修改仓位")
        self.setMinimumWidth(300)

        self.symbol_combo = ComboBox()
        self.symbol_combo.pop_show.connect(self.refresh_symbol_list)
        self.symbol_combo.activated[str].connect(self.set_modify_symbol)

        validator = QtGui.QIntValidator()

        # self.long_pos_line = QtWidgets.QLineEdit()
        # self.long_pos_line.setValidator(validator)
        # self.short_pos_line = QtWidgets.QLineEdit()
        # self.short_pos_line.setValidator(validator)

        self.basic_delta_line = QtWidgets.QLineEdit()
        self.basic_delta_line.setValidator(validator)

        self.traded_net_line = QtWidgets.QLineEdit()
        self.traded_net_line.setValidator(validator)

        self.lost_follow_line = QtWidgets.QLineEdit()
        self.lost_follow_line.setValidator(validator)

        button_modify = QtWidgets.QPushButton("修改")
        button_modify.clicked.connect(self.modify)

        for btn in [button_modify]:
            btn.setFixedHeight(btn.sizeHint().height() * 1.5)

        form = QtWidgets.QFormLayout()
        form.addRow("合约代码", self.symbol_combo)
        # form.addRow("目标户多仓", self.long_pos_line)
        # form.addRow("目标户空仓", self.short_pos_line)
        form.addRow("底仓差", self.basic_delta_line)
        form.addRow("交易净仓", self.traded_net_line)
        form.addRow("丢失净仓", self.lost_follow_line)

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
        # target_long = self.follow_engine.get_pos(vt_symbol, 'target_long')
        # target_short = self.follow_engine.get_pos(vt_symbol, 'target_short')
        basic_delta = self.follow_engine.get_pos(vt_symbol, 'basic_delta')
        traded_net = self.follow_engine.get_pos(vt_symbol, 'source_traded_net')
        lost_follow = self.follow_engine.get_pos(vt_symbol, 'lost_follow_net')

        # self.long_pos_line.setText(str(target_long))
        # self.short_pos_line.setText(str(target_short))
        self.basic_delta_line.setText(str(basic_delta))
        self.traded_net_line.setText(str(traded_net))
        self.lost_follow_line.setText(str(lost_follow))
        self.write_log(f"选中合约{self.modify_symbol}")

    def refresh_symbol_list(self):
        """"""
        self.symbol_combo.clear()
        symbol_list = list(self.follow_engine.get_positions().keys())
        self.symbol_combo.addItems(symbol_list)

    def modify(self):
        """"""
        # new_long = self.long_pos_line.text()
        # new_short = self.short_pos_line.text()
        new_basic_delta = self.basic_delta_line.text()
        new_traded_net = self.traded_net_line.text()
        new_lost_follow = self.lost_follow_line.text()

        modify_pos_dict = dict()
        modify_pos_dict['basic_delta'] = int(new_basic_delta)
        modify_pos_dict['source_traded_net'] = int(new_traded_net)
        modify_pos_dict['lost_follow_net'] = int(new_lost_follow)
        modify_pos_tuple = (self.modify_symbol, modify_pos_dict)

        event = Event(EVENT_FOLLOW_MODIFY_POS, modify_pos_tuple)
        self.follow_engine.event_engine.put(event)

    def write_log(self, msg: str):
        """"""
        self.follow_engine.write_log(msg)


class OrderSettingEditor(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, follow_engine: FollowEngine):
        super().__init__()
        self.parent = parent
        self.follow_engine = follow_engine
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("委托设置")
        self.setMinimumWidth(300)

        self.order_type_combo = QtWidgets.QComboBox()
        self.order_type_combo.addItems(['限价', '市价'])
        self.order_type_combo.activated[str].connect(self.set_order_type)
        self.get_current_order_type()

        self.chase_base_price_combo = QtWidgets.QComboBox()
        self.chase_base_price_combo.addItems(['对手价', '挂单价'])
        self.chase_base_price_combo.activated[str].connect(self.set_chase_base_price)
        self.get_current_chase_base_price()

        self.sync_base_price_combo = QtWidgets.QComboBox()
        self.sync_base_price_combo.addItems(['对手价', '挂单价'])
        self.sync_base_price_combo.activated[str].connect(self.set_sync_base_price)
        self.get_current_sync_base_price()

        self.chase_combo = QtWidgets.QComboBox()
        self.chase_combo.addItems(['是', '否'])
        self.chase_combo.activated[str].connect(self.set_is_chase)

        self.chase_base_last_order_combo = QtWidgets.QComboBox()
        self.chase_base_last_order_combo.addItems(['是', '否'])
        self.chase_base_last_order_combo.activated[str].connect(self.set_chase_base_last)

        self.keep_order_after_chase_combo = QtWidgets.QComboBox()
        self.keep_order_after_chase_combo.addItems(['是', '否'])
        self.keep_order_after_chase_combo.activated[str].connect(self.set_keep_order_after_chase)

        validator = QtGui.QIntValidator()
        self.chase_timeout_line = QtWidgets.QLineEdit(str(self.follow_engine.chase_order_timeout))
        self.chase_timeout_line.setValidator(validator)
        self.chase_timeout_line.editingFinished.connect(self.set_chase_order_timeout)

        self.chase_tickadd_line = QtWidgets.QLineEdit(str(self.follow_engine.chase_order_tick_add))
        self.chase_tickadd_line.setValidator(validator)
        self.chase_tickadd_line.editingFinished.connect(self.set_chase_order_tickadd)

        self.chase_resend_line = QtWidgets.QLineEdit(str(self.follow_engine.chase_max_resend))
        self.chase_resend_line.setValidator(validator)
        self.chase_resend_line.editingFinished.connect(self.set_chase_max_resend)

        self.timeout_line = QtWidgets.QLineEdit(str(self.follow_engine.cancel_order_timeout))
        self.timeout_line.setValidator(validator)
        self.timeout_line.editingFinished.connect(self.set_cancel_order_timeout)

        self.tickout_line = QtWidgets.QLineEdit(str(self.follow_engine.tick_add))
        self.tickout_line.setValidator(validator)
        self.tickout_line.editingFinished.connect(self.set_tick_add)

        self.mustdone_tickout_line = QtWidgets.QLineEdit(str(self.follow_engine.must_done_tick_add))
        self.mustdone_tickout_line.setValidator(validator)
        self.mustdone_tickout_line.editingFinished.connect(self.set_must_done_tick_add)

        self.single_max_line = QtWidgets.QLineEdit(str(self.follow_engine.single_max))
        self.single_max_line.setValidator(validator)
        self.single_max_line.editingFinished.connect(self.set_single_max)

        self.chase_combo.currentIndexChanged[int].connect(self.change_chase_op_editable)
        self.get_current_chase()
        self.chase_base_last_order_combo.currentIndexChanged[int].connect(self.change_base_price_editable)
        self.get_current_chase_base_last()

        self.get_current_keep_order_after_chase()

        self.save_setting_button = QtWidgets.QPushButton("保存设置")
        self.save_setting_button.clicked.connect(self.save_setting)
        self.save_setting_button.setFixedHeight(self.save_setting_button.sizeHint().height() * 1.5)

        form = QtWidgets.QFormLayout()
        form.addRow("发单类型", self.order_type_combo)
        form.addRow("超时撤单(秒)", self.timeout_line)
        form.addRow("小超价档位", self.tickout_line)
        form.addRow("大超价档位", self.mustdone_tickout_line)
        form.addRow("单笔最大手数", self.single_max_line)
        form.addRow("是否追单", self.chase_combo)
        form.addRow("是否基于上笔委托追单", self.chase_base_last_order_combo)
        form.addRow("追单基础价(不指定)", self.chase_base_price_combo)
        form.addRow("追单超时", self.chase_timeout_line)
        form.addRow("追单超价", self.chase_tickadd_line)
        form.addRow("最大追单次数", self.chase_resend_line)
        form.addRow("追单失败后保留委托", self.keep_order_after_chase_combo)
        form.addRow("手动同步基础价", self.sync_base_price_combo)
        form.addRow(self.save_setting_button)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(form)

        self.setLayout(vbox)

    def get_current_order_type(self):
        """"""
        order_type = self.follow_engine.order_type
        self.order_type_combo.setCurrentText(order_type.value)

    def get_current_chase_base_price(self):
        base_price = self.follow_engine.chase_base_price
        self.chase_base_price_combo.setCurrentText(base_price.value)

    def get_current_sync_base_price(self):
        base_price = self.follow_engine.sync_base_price
        self.sync_base_price_combo.setCurrentText(base_price.value)

    def get_current_chase(self):
        """"""
        is_chase_order = self.follow_engine.is_chase_order
        if is_chase_order:
            self.chase_combo.setCurrentIndex(0)
        else:
            self.chase_combo.setCurrentIndex(1)

    def get_current_chase_base_last(self):
        is_chase_base_last = self.follow_engine.chase_base_last_order_price
        if is_chase_base_last:
            self.chase_base_last_order_combo.setCurrentIndex(0)
            self.chase_base_price_combo.setEnabled(False)
        else:
            self.chase_base_last_order_combo.setCurrentIndex(1)
            self.chase_base_price_combo.setEnabled(True)

    def get_current_keep_order_after_chase(self):
        is_keep_order = self.follow_engine.is_keep_order_after_chase
        if is_keep_order:
            self.keep_order_after_chase_combo.setCurrentIndex(0)
        else:
            self.keep_order_after_chase_combo.setCurrentIndex(1)

    def change_base_price_editable(self, editable: int):
        if not editable:
            self.chase_base_price_combo.setEnabled(False)
        else:
            self.chase_base_price_combo.setEnabled(True)

    def change_chase_op_editable(self, is_chase: int):
        print(f'is chase:{is_chase}')
        if is_chase == 0:
            self.chase_base_last_order_combo.setEnabled(True)
            self.chase_timeout_line.setEnabled(True)
            self.chase_tickadd_line.setEnabled(True)
            self.chase_resend_line.setEnabled(True)
        else:
            self.chase_base_last_order_combo.setEnabled(False)
            self.chase_timeout_line.setEnabled(False)
            self.chase_tickadd_line.setEnabled(False)
            self.chase_resend_line.setEnabled(False)

    def set_order_type(self, order_type: str):
        """"""
        if order_type == "限价":
            self.follow_engine.set_parameters('order_type', OrderType.LIMIT)
        else:
            self.follow_engine.set_parameters('order_type', OrderType.MARKET)
        self.write_log(f"发单类型：{self.follow_engine.order_type.value} 设置成功")

    def set_chase_base_price(self, chase_base_price: str):
        if chase_base_price == "挂单价":
            self.follow_engine.set_parameters('chase_base_price', OrderBasePrice.GOOD_FOR_SELF)
        else:
            self.follow_engine.set_parameters('chase_base_price', OrderBasePrice.GOOD_FOR_OTHER)
        self.write_log(f"追单基础价（不指定）：{self.follow_engine.chase_base_price.value} 设置成功")

    def set_sync_base_price(self, sync_base_price: str):
        if sync_base_price == "挂单价":
            self.follow_engine.set_parameters('sync_base_price', OrderBasePrice.GOOD_FOR_SELF)
        else:
            self.follow_engine.set_parameters('sync_base_price', OrderBasePrice.GOOD_FOR_OTHER)
        self.write_log(f"手动同步基础价：{self.follow_engine.sync_base_price.value} 设置成功")


    def set_is_chase(self, chase_flag: str):
        """"""
        if chase_flag == "是":
            self.follow_engine.set_parameters('is_chase_order', True)
            self.chase_base_last_order_combo.setEnabled(True)
            self.chase_timeout_line.setEnabled(True)
            self.chase_tickadd_line.setEnabled(True)
            self.chase_resend_line.setEnabled(True)
        else:
            self.follow_engine.set_parameters('is_chase_order', False)
            self.chase_base_last_order_combo.setEnabled(False)
            self.chase_timeout_line.setEnabled(False)
            self.chase_tickadd_line.setEnabled(False)
            self.chase_resend_line.setEnabled(False)
        self.write_log(f"是否追单：{self.follow_engine.is_chase_order}")

    def set_chase_base_last(self, chase_flag: str):
        if chase_flag == "是":
            self.follow_engine.set_parameters('chase_base_last_order_price', True)
            self.chase_base_price_combo.setEnabled(False)
        else:
            self.follow_engine.set_parameters('chase_base_last_order_price', False)
            self.chase_base_price_combo.setEnabled(True)
        self.write_log(f"追价是否基于上笔委托价格：{self.follow_engine.chase_base_last_order_price}")

    def set_keep_order_after_chase(self, keep_order_flag: str):
        if keep_order_flag == "是":
            self.follow_engine.set_parameters('is_keep_order_after_chase', True)
        else:
            self.follow_engine.set_parameters('is_keep_order_after_chase', False)
        self.write_log(f"追单失败后是否保留委托：{self.follow_engine.is_keep_order_after_chase}")

    def set_chase_order_timeout(self):
        """"""
        text = self.chase_timeout_line.text()
        self.follow_engine.set_parameters('chase_order_timeout', int(text))
        self.write_log(f"追价超时自动撤单：{self.follow_engine.chase_order_timeout} 秒设置成功")

    def set_chase_order_tickadd(self):
        """"""
        text = self.chase_tickadd_line.text()
        self.follow_engine.set_parameters('chase_order_tick_add', int(text))
        self.write_log(f"追价超价档位：{self.follow_engine.chase_order_tick_add} 设置成功")

    def set_chase_max_resend(self):
        """"""
        text = self.chase_resend_line.text()
        self.follow_engine.set_parameters('chase_max_resend', int(text))
        self.write_log(f"最大追价次数：{self.follow_engine.chase_max_resend} 设置成功")

    def set_cancel_order_timeout(self):
        """"""
        text = self.timeout_line.text()
        self.follow_engine.set_parameters('cancel_order_timeout', int(text))
        self.write_log(f"未成交自动撤单超时：{self.follow_engine.cancel_order_timeout} 秒设置成功")

    def set_tick_add(self):
        """"""
        text = self.tickout_line.text()
        self.follow_engine.set_parameters('tick_add', int(text))
        self.write_log(f"小超价档位：{self.follow_engine.tick_add} 设置成功")

    def set_must_done_tick_add(self):
        text = self.mustdone_tickout_line.text()
        self.follow_engine.set_parameters('must_done_tick_add', int(text))
        self.write_log(f"大超价档位：{self.follow_engine.must_done_tick_add} 设置成功")

    def set_single_max(self):
        """"""
        text = self.single_max_line.text()
        self.follow_engine.set_parameters('single_max', int(text))
        self.write_log(f"单笔最大手数：{self.follow_engine.single_max} 设置成功")

    def save_setting(self):
        self.follow_engine.save_follow_setting()

    def write_log(self, msg: str):
        """"""
        self.follow_engine.write_log(msg)


class SkipContractEditor(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, follow_engine: FollowEngine):
        super().__init__()

        self.parent = parent
        self.follow_engine = follow_engine
        self.removed_symbol = ''
        self.removed_com = ''
        self.removed_vol = 0

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("同步合约设置")
        self.setMinimumWidth(300)

        self.symbol_combo = ComboBox()
        self.symbol_combo.pop_show.connect(self.refresh_symbol_list)
        self.symbol_combo.activated[str].connect(self.set_removed_symbol)
        self.refresh_symbol_list()

        self.intra_combo = ComboBox()
        self.intra_combo.pop_show.connect(self.refresh_intra_list)
        self.intra_combo.activated[str].connect(self.set_removed_com)
        self.refresh_intra_list()

        self.order_vol_combo = ComboBox()
        self.order_vol_combo.pop_show.connect(self.refresh_order_vol_white_list)
        self.order_vol_combo.activated[str].connect(self.set_removed_order_vol)
        self.refresh_order_vol_white_list()

        self.filter_vol_combo = QtWidgets.QComboBox()
        self.filter_vol_combo.addItems(['是', '否'])
        self.filter_vol_combo.activated[str].connect(self.set_is_filter_vol)
        self.get_current_filter_vol()

        validator = QtGui.QIntValidator()
        self.new_remove_line = QtWidgets.QLineEdit()
        self.new_intra_line = QtWidgets.QLineEdit()
        
        self.new_order_vol_line = QtWidgets.QLineEdit()
        self.new_order_vol_line.setValidator(validator)

        button_add = QtWidgets.QPushButton("添加禁止同步")
        button_add.clicked.connect(self.add)

        button_remove = QtWidgets.QPushButton("移除禁止同步")
        button_remove.clicked.connect(self.remove)

        button_add_com = QtWidgets.QPushButton("添加日内品种")
        button_add_com.clicked.connect(self.add_com)

        button_remove_com = QtWidgets.QPushButton("移除日内品种")
        button_remove_com.clicked.connect(self.remove_com)

        button_add_order_volume = QtWidgets.QPushButton("添加委托手数")
        button_add_order_volume.clicked.connect(self.add_order_volume)

        button_remove_order_volume = QtWidgets.QPushButton("移除委托手数")
        button_remove_order_volume.clicked.connect(self.remove_order_volume)

        big_btns = [
            button_add, button_remove,
            button_add_com, button_remove_com,
            button_add_order_volume, button_remove_order_volume
        ]

        for btn in big_btns:
            btn.setFixedHeight(btn.sizeHint().height() * 1)

        save_setting_button = QtWidgets.QPushButton("保存设置")
        save_setting_button.clicked.connect(self.save_setting)
        save_setting_button.setFixedHeight(save_setting_button.sizeHint().height() * 1.5)

        form = QtWidgets.QFormLayout()
        form.addRow("禁止同步合约", self.symbol_combo)
        form.addRow("添加新合约", self.new_remove_line)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(button_add)
        hbox.addWidget(button_remove)

        form_com = QtWidgets.QFormLayout()
        form_com.addRow("日内模式品种", self.intra_combo)
        form_com.addRow("添加新品种", self.new_intra_line)

        hbox_com = QtWidgets.QHBoxLayout()
        hbox_com.addWidget(button_add_com)
        hbox_com.addWidget(button_remove_com)

        form_order_vol = QtWidgets.QFormLayout()
        form_order_vol.addRow("是否过滤委托手数", self.filter_vol_combo)
        form_order_vol.addRow("允许跟单委托手数", self.order_vol_combo)
        form_order_vol.addRow("添加新手数", self.new_order_vol_line)

        hbox_order_vol = QtWidgets.QHBoxLayout()
        hbox_order_vol.addWidget(button_add_order_volume)
        hbox_order_vol.addWidget(button_remove_order_volume)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(form)
        vbox.addLayout(hbox)
        vbox.addLayout(form_com)
        vbox.addLayout(hbox_com)
        vbox.addLayout(form_order_vol)
        vbox.addLayout(hbox_order_vol)

        vbox.addWidget(save_setting_button)

        self.setLayout(vbox)

        # self.symbol_combo.currentTextChanged[str].connect(self.set_removed_symbol)
        # self.intra_combo.currentTextChanged[str].connect(self.set_removed_com)

    def set_removed_symbol(self, vt_symbol: str):
        """
        Set symbol to be modified
        """
        self.new_remove_line.setText(vt_symbol)
        self.removed_symbol = vt_symbol
        self.write_log(f"选中合约名{self.removed_symbol}")

    def set_removed_com(self, commodity: str):
        """
        Set commodity to intraday mode
        """
        self.new_intra_line.setText(commodity)
        self.removed_com = commodity
        self.write_log(f"选中品种名{self.removed_com}")

    def set_removed_order_vol(self, volume: str):
        """"""
        self.new_order_vol_line.setText(volume)
        self.removed_vol = int(volume)
        self.write_log(f"选中委托手数{self.removed_vol}")

    def refresh_symbol_list(self):
        """"""
        self.symbol_combo.clear()
        symbol_list = self.follow_engine.get_skip_contracts()
        self.symbol_combo.addItems(symbol_list)

    def refresh_intra_list(self):
        self.intra_combo.clear()
        symbol_list = self.follow_engine.get_intraday_symbols()
        self.intra_combo.addItems(symbol_list)

    def refresh_order_vol_white_list(self):
        self.order_vol_combo.clear()
        vol_list = self.follow_engine.get_order_vols_to_follow()
        vol_str_list = [str(vol) for vol in vol_list]
        self.order_vol_combo.addItems(vol_str_list)

    def add(self):
        """"""
        vt_symbol = self.new_remove_line.text()
        if self.parent.validate_vt_symbol(vt_symbol):
            if vt_symbol not in self.follow_engine.get_skip_contracts():
                self.follow_engine.get_skip_contracts().append(vt_symbol)
                self.refresh_symbol_list()
                self.parent.refresh_skip_contracts()
                self.write_log(f"{vt_symbol}添加到禁止同步合约成功")
            else:
                self.write_log(f"{vt_symbol}已禁止同步，无需重复添加")

    def remove(self):
        """"""
        vt_symbol = self.removed_symbol
        if vt_symbol:
            skip_contracts = self.follow_engine.get_skip_contracts()
            if vt_symbol in skip_contracts:
                skip_contracts.remove(vt_symbol)
                self.refresh_symbol_list()
                self.parent.refresh_skip_contracts()
                self.write_log(f"{vt_symbol}从禁止同步合约移除成功")
        else:
            self.write_log(f"合约尚未选择")

    def add_com(self):
        """"""
        commodity = self.new_intra_line.text()
        if not commodity:
            self.write_log(f"未输入日内品种代码")
            return

        if commodity not in self.follow_engine.get_intraday_symbols():
            self.follow_engine.get_intraday_symbols().append(commodity)
            self.refresh_intra_list()
            self.parent.refresh_intraday()
            self.write_log(f"{commodity}添加到日内模式成功")
        else:
            self.write_log(f"{commodity}已成为日内模式，无需重复添加")

    def remove_com(self):
        """"""
        commodity = self.removed_com
        if commodity:
            intra_symbols = self.follow_engine.get_intraday_symbols()
            if commodity in intra_symbols:
                intra_symbols.remove(commodity)
                self.refresh_intra_list()
                self.parent.refresh_intraday()
                self.write_log(f"{commodity}从日内模式移除成功")
        else:
            self.write_log(f"品种尚未选择")

    def add_order_volume(self):
        vol = self.new_order_vol_line.text()
        if not vol:
            self.write_log(f"未输入允许跟随的委托手数")
            return

        if int(vol) not in self.follow_engine.get_order_vols_to_follow():
            self.follow_engine.get_order_vols_to_follow().append(int(vol))
            self.refresh_order_vol_white_list()
            self.parent.refresh_order_vols()
            self.write_log(f"委托手数：{vol}添加到跟单手数成功")
            print(self.follow_engine.get_order_vols_to_follow())
        else:
            self.write_log(f"委托手数：{vol}无需重复添加")

    def remove_order_volume(self):
        order_vol = self.removed_vol
        if order_vol:
            vol_list = self.follow_engine.get_order_vols_to_follow()
            if order_vol in vol_list:
                vol_list.remove(order_vol)
                self.refresh_order_vol_white_list()
                self.parent.refresh_order_vols()
                self.write_log(f"{order_vol}从跟单委托手数移除成功")
                print(self.follow_engine.get_order_vols_to_follow())
        else:
            self.write_log(f"手数尚未选择")

    def set_is_filter_vol(self, chase_flag: str):
        """"""
        if chase_flag == "是":
            self.follow_engine.set_parameters('is_filter_order_vol', True)
        else:
            self.follow_engine.set_parameters('is_filter_order_vol', False)
        self.write_log(f"是否过滤委托手数：{self.follow_engine.is_filter_order_vol}")

    def get_current_filter_vol(self):
        """"""
        is_filter_order_vol = self.follow_engine.is_filter_order_vol
        if is_filter_order_vol:
            self.filter_vol_combo.setCurrentIndex(0)
        else:
            self.filter_vol_combo.setCurrentIndex(1)

    def save_setting(self):
        self.follow_engine.save_follow_setting()

    def write_log(self, msg: str):
        """"""
        self.follow_engine.write_log(msg)


class CloseHedgedDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, follow_engine: FollowEngine):
        super().__init__()

        self.parent = parent
        self.follow_engine = follow_engine
        self.close_symbol = ""

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("锁仓单平仓")
        self.setMinimumWidth(300)

        self.symbol_combo = ComboBox()
        self.symbol_combo.pop_show.connect(self.refresh_symbol_list)
        self.symbol_combo.activated[str].connect(self.set_close_symbol)

        validator = QtGui.QIntValidator()
        self.close_pos_line = QtWidgets.QLineEdit()
        self.close_pos_line.setValidator(validator)

        button_close = QtWidgets.QPushButton("平仓")
        button_close.clicked.connect(self.close_hedged_pos)

        for btn in [button_close]:
            btn.setFixedHeight(btn.sizeHint().height() * 1.5)

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
