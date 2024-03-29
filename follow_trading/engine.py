import pickle
import traceback
import pandas as pd

from datetime import datetime, timedelta, time
from enum import Enum
from copy import copy
from dataclasses import dataclass
from typing import Optional, Tuple, Union

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.utility import load_json, save_json, get_folder_path, get_file_path
from vnpy.trader.converter import OffsetConverter
from vnpy.trader.constant import (
    OrderType,
    Direction,
    Offset,
    Status
)
from vnpy.trader.event import (
    EVENT_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_TIMER,
    EVENT_LOG
)
from vnpy.trader.object import (
    OrderRequest,
    SubscribeRequest,
    LogData,
    TickData,
    TradeData,
    OrderData,
    PositionData
)


@dataclass
class PosDeltaData:
    vt_symbol: str = ""
    source_long: int = 0
    source_short: int = 0
    source_net: int = 0
    target_long: int = 0
    target_short: int = 0
    target_net: int = 0
    long_delta: int = 0
    short_delta: int = 0
    net_delta: int = 0
    basic_delta: int = 0
    source_traded_net: int = 0
    target_traded_net: int = 0


class FollowRunType(Enum):
    TEST = "测试"
    LIVE = "实盘"


class TradeType(Enum):
    BUY = "买开"
    SHORT = "卖开"
    SELL = "卖平"
    COVER = "买平"


class OrderBasePrice(Enum):
    GOOD_FOR_OTHER = "对手价"
    GOOD_FOR_SELF = "挂单价"


class FollowBaseMode(Enum):
    BASE_ORDER = "跟随委托"
    BASE_TRADE = "跟随成交"


APP_NAME = "FollowTrading"

EVENT_FOLLOW_LOG = "eFollowLog"
EVENT_FOLLOW_POS_DELTA = "eFollowPosDelta"
EVENT_FOLLOW_ORDER = "eFollowOrder"
EVENT_FOLLOW_MODIFY_POS = "eFollowModifyPos"

DAYLIGHT_MARKET_END = time(15, 2)
NIGHT_MARKET_BEGIN = time(20, 45)


class FollowEngine(BaseEngine):
    """
    If following symbol is not intraday mode, The trade can follow many account send to 1 account.
    If following symbol is intraday mode. The trade can only one to one
    """
    setting_filename = "follow_trading_setting.json"
    data_filename = "follow_trading_data.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__(main_engine, event_engine, APP_NAME)

        #### 参数 ###
        self.source_gateway_name = "CTP"
        self.target_gateway_name = "RPC"
        self.filter_trade_timeout = 60
        self.cancel_order_timeout = 10
        self.max_cancel = 3
        self.multiples = 1
        self.follow_based = FollowBaseMode.BASE_TRADE

        self.sync_base_price = OrderBasePrice.GOOD_FOR_OTHER

        self.tick_add = 5
        self.must_done_tick_add = 25

        self.is_chase_order = False
        self.chase_base_last_order_price = True
        self.chase_base_price = OrderBasePrice.GOOD_FOR_SELF
        self.chase_order_tick_add = 5
        self.chase_order_timeout = 10
        self.chase_max_resend = 3
        self.is_keep_order_after_chase = False

        self.is_intraday_trading = False    # 改为默认非日内模式
        self.inverse_follow = False
        self.order_type = OrderType.LIMIT

        self.single_max = 1000
        self.single_max_dict = {
            "IF": 20,
            "IC": 20,
            "IH": 20
        }

        self.intraday_symbols = []
        self.skip_contracts = []

        self.is_filter_order_vol = True
        self.order_volumes_to_follow = [1, 2]

        # 运行模式
        self.run_type = FollowRunType.LIVE
        # 测试模式参数
        self.test_symbol = ''
        self.test_count = 0
        self.tick_time = None

        #### 变量 ###
        self.gateway_names = None
        self.is_active = False

        self.follow_data = {}
        self.follow_setting = {}

        # 主要容器变量
        self.tradeid_orderids_dict = {}         # vt_tradeid: list[vt_orderid]
        self.positions = {}
        self.vt_tradeids = set()
        self.due_out_req_list = []
        self.orderid_to_signal_orderid = {}     # vt_orderid: vt_orderid or vt_tradeid

        # 委托模式变量
        self.vt_accepted_orderids = set()
        self.orderid_keep_hang = set()
        self.fail_chase_orderid = set()

        # 市场数据初始化变量
        self.pre_subscribe_symbols = set()
        self.limited_prices = {}
        self.latest_prices = {}

        # 净量交易相关变量
        self.intraday_orderids = set()
        self.open_orderids = set()

        # 追单变量
        self.first_orderids = set()
        self.chase_orderids = set()
        self.chase_ancestor_dict = {}           # vt_orderid: vt_orderid
        self.chase_resend_count_dict = {}       # vt_orderid: int

        # 超时撤单变量
        self.active_order_set = set()
        self.active_order_counter = {}          # vt_orderid: int
        self.cancel_counter = {}                # vt_orderid: int

        # 其它
        self.is_hedged_closed = False
        self.is_trade_saved = False
        self.sync_order_ref = 0
        self.refresh_pos_interval = 0

        self.offset_converter = OffsetConverter(main_engine)

        # 参数如果是python object不能直接转化为json数据
        self.parameters = [
                           'source_gateway_name', 'target_gateway_name',
                           'filter_trade_timeout', 'cancel_order_timeout',
                           'multiples', 'follow_based', 'sync_base_price', 'is_keep_order_after_chase',
                           'tick_add', 'must_done_tick_add',
                           'inverse_follow',
                           'order_type', 'run_type',
                           'test_symbol', 'intraday_symbols',
                           'skip_contracts',
                           'single_max', 'single_max_dict',
                           'is_chase_order', 'chase_base_price', 'chase_base_last_order_price',
                           'chase_order_timeout', 'chase_order_tick_add', 'chase_max_resend',
                           'is_intraday_trading', 'is_filter_order_vol', 'order_volumes_to_follow'
                           ]
        self.variables = ['tradeid_orderids_dict', 'positions']
        self.clear_variables = ['tradeid_orderids_dict']
        self.pos_key = [
                        'source_long', 'source_short', 'source_net',
                        'target_long', 'target_short', 'target_net',
                        'net_delta', 'basic_delta',
                        'source_traded_net',
                        'lost_follow_net'
                        ]

        self.load_data()
        self.view_vars()

        print("版本最后修改时间: 20230315 14:00")

    def init_engine(self):
        """
        Init engine.
        """
        self.write_log("参数和数据读取成功。")

        # 重要：启动前必须先更新已经记录过的tradeid
        self.update_tradeids()
        # print('vt_tradeids', self.vt_tradeids)
        self.register_event()

        if self.run_type == FollowRunType.TEST:
            self.write_log("测试模式：订阅行情以获取最新时间。")
            self.subscribe(self.test_symbol)
        else:
            self.write_log("实盘模式：定期校时以确保时间准确。")
        self.write_log("跟随交易初始化完成。")

    def load_data(self):
        """
        Load variables and settings
        """
        self.load_follow_setting()
        self.load_follow_data()

    def get_current_time(self):
        """"""
        return datetime.now()

    def set_gateways(self, source_name: str, target_name: str):
        """
        Set gateway names.
        """
        self.source_gateway_name = source_name
        self.target_gateway_name = target_name

    def set_parameters(self, param_name, value):
        """"""
        setattr(self, param_name, value)

    def get_pos(self, vt_symbol: str, name: str):
        """"""
        symbol_pos = self.positions.get(vt_symbol, None)
        if symbol_pos:
            return symbol_pos[name]

    def set_pos(self, vt_symbol: str, name: str, pos: int):
        """"""
        symbol_pos = self.get_symbol_pos(vt_symbol)
        symbol_pos[name] = pos

    def get_connected_gateway_names(self):
        """
        Get connected gateway names.
        """
        accounts = self.main_engine.get_all_accounts()
        print(accounts)
        self.gateway_names = [account.gateway_name for account in accounts]
        print(self.gateway_names)
        return self.gateway_names

    def get_positions(self):
        """"""
        return self.positions

    def get_skip_contracts(self):
        """"""
        return self.skip_contracts

    def get_intraday_symbols(self):
        """"""
        return self.intraday_symbols

    def get_order_vols_to_follow(self):
        return self.order_volumes_to_follow

    def load_follow_setting(self):
        """
        Load setting from setting file.
        """
        self.follow_setting = load_json(self.setting_filename)
        print(self.follow_setting)
        for name in self.parameters:
            value = self.follow_setting.get(name, None)
            if value is not None:
                if name == 'order_type':
                    setattr(self, name, OrderType(value))
                elif name == 'run_type':
                    setattr(self, name, FollowRunType(value))
                elif name == "chase_base_price":
                    setattr(self, name, OrderBasePrice(value))
                elif name == "follow_based":
                    setattr(self, name, FollowBaseMode(value))
                elif name == "sync_base_price":
                    setattr(self, name, OrderBasePrice(value))
                else:
                    setattr(self, name, value)
        self.write_log("参数配置读取成功。")

    def save_follow_setting(self):
        """
        Save follow setting to setting file.
        """
        for name in self.parameters:
            if name in ['order_type', 'run_type', 'chase_base_price', 'follow_based', 'sync_base_price']:
                self.follow_setting[name] = getattr(self, name).value   # noqa
            else:
                self.follow_setting[name] = getattr(self, name)
        save_json(self.setting_filename, self.follow_setting)
        self.write_log("参数配置存储成功。")

    def load_follow_data(self):
        """
        Load run data from data file.
        """
        self.follow_data = load_json(self.data_filename)
        for name in self.variables:
            value = self.follow_data.get(name, None)
            if value:
                setattr(self, name, value)
        self.write_log("运行数据读取成功。")

    def save_follow_data(self):
        """
        Save run data to data file.
        """
        for name in self.variables:
            self.follow_data[name] = getattr(self, name)
        save_json(self.data_filename, self.follow_data)

    def clear_follow_data(self):
        """
        Clear follow data after market closed
        """
        if self.follow_data:
            # 如果数据文件不存在就保存到历史文件
            today = datetime.now().strftime('%Y%m%d')
            fn = f"follow_history/{today}_{self.data_filename}"
            fp = get_file_path(fn)
            if not fp.exists():
                save_json(fn, self.follow_data)
                self.write_log("清除临时数据并保存至历史成功。")
            else:
                self.write_log("已有历史临时数据文件，无需覆盖。")

            # 清理临时变量
            for name in self.clear_variables:
                self.follow_data[name].clear()
            save_json(self.data_filename, self.follow_data)

    def save_trade(self):
        """
        Save trade record to file.
        """
        today = datetime.now().strftime('%Y%m%d')
        trade_folder = get_folder_path('trade')
        trade_file_path = trade_folder.joinpath(f"trade_{today}.csv")

        gateway_dict = dict()
        gateway_dict[self.source_gateway_name] = "source"
        gateway_dict[self.target_gateway_name] = "target"

        account_dict = dict()
        accounts = self.main_engine.get_all_accounts()
        for account in accounts:
            account_dict[account.gateway_name] = account.accountid

        trades = self.main_engine.get_all_trades()
        trade_list = []
        for trade in trades:
            d = copy(trade.__dict__)
            d["exchange"] = d["exchange"].value
            d["direction"] = d["direction"].value
            d["offset"] = d["offset"].value

            try:
                d['dt'] = f"{today} {d['time']}"
            except KeyError:
                d['dt'] = d['datetime'].strftime("%Y%m%d %H:%M:%S")

            d['date'] = f"{today}"
            d.pop("vt_symbol")
            trade_list.append(d)
        df = pd.DataFrame(trade_list)
        if not df.empty:
            df['account_type'] = df['gateway_name'].map(gateway_dict)
            df['account_id'] = df['gateway_name'].map(account_dict)
            df.to_csv(trade_file_path, index=False, encoding='utf-8')
            self.write_log("成交记录保存成功。")

    def save_account_info(self):
        """
        Save account info to file every day
        """
        today = datetime.now().strftime('%Y%m%d')
        account_file = get_file_path("account_info.csv")

        account_text = ""
        accounts = self.main_engine.get_all_accounts()
        for account in accounts:
            txt_ = f"{today},{account.accountid},{account.balance},{account.available}\n"
            account_text += txt_
        with open(account_file, "a+", encoding="utf-8") as f:
            f.write(account_text)
        self.write_log("账户信息保存成功。")

    def update_tradeids(self):
        """
        Update received tradeids from main engine
        """
        trades = self.main_engine.get_all_trades()
        tradeids = [trade.vt_tradeid for trade in trades]
        self.vt_tradeids.update(set(tradeids))
        self.write_log("成交单列表更新成功。")

    def auto_save_trade(self):
        """
        Auto saved sorts of info after market closed, only allow run once.
        """
        if self.is_trade_saved:
            return

        now_time = datetime.now().time()
        if NIGHT_MARKET_BEGIN > now_time >= DAYLIGHT_MARKET_END:
            self.save_trade()
            self.clear_follow_data()
            self.save_account_info()

            self.is_trade_saved = True

    def start(self):
        """
        Start follow trading.
        """
        if self.is_active:
            self.write_log("跟随交易运行中。")
            return False

        if self.source_gateway_name == self.target_gateway_name:
            self.write_log("跟随接口和发单接口不能是同一个。")
            return False

        self.is_active = True
        self.write_log("跟随交易启动。")

        return True

    def stop(self):
        """
        Stop follow trading.
        """
        if not self.is_active:
            self.write_log("跟随交易尚未启动。")
            return False

        self.is_active = False

        # 停止系统是否撤单
        # self.cancel_all_order()

        self.write_log("跟随交易停止。")

        self.clear_empty_pos()
        self.clear_expired_pos()

        self.save_follow_setting()
        self.save_follow_data()

        self.save_trade()

        # 停止系统是否保存合约列表
        # self.save_contract()

        now_time = datetime.now().time()
        if NIGHT_MARKET_BEGIN > now_time >= DAYLIGHT_MARKET_END:
            self.clear_follow_data()
            self.save_account_info()
        return True

    def close(self):
        """
        Close engine.
        """
        self.stop()

    def save_contract(self):
        """
        For Test Only
        """
        contracts = self.main_engine.get_all_contracts()
        filepath = get_file_path('contracts.data')
        with open(filepath, 'wb') as f:
            pickle.dump(contracts, f)
        self.write_log(f"当日合约数据保存成功。")

    @staticmethod
    def get_trade_net_vol(trade: TradeData):
        """"""
        if trade.direction == Direction.LONG:
            vol = trade.volume
        else:
            vol = -trade.volume
        return vol

    @staticmethod
    def get_req_net_vol(req: OrderRequest):
        """"""
        if req.direction == Direction.LONG:
            vol = req.volume
        else:
            vol = -req.volume
        return vol

    @staticmethod
    def get_trade_dict(trade: TradeData, is_must_done: bool):
        """
        Merge trade and is_must_done flag to dict
        """
        d = {}
        d['trade'] = trade
        d['is_must_done'] = is_must_done
        return d

    @staticmethod
    def get_trade_time(trade: Union[OrderData, TradeData]):
        """
        Get trade/order time for compartility with old version
        """
        try:
            trade_time = trade.time
        except AttributeError:
            trade_time = trade.datetime.strftime("%H:%M:%S")
        return trade_time

    @staticmethod
    def get_trade_type(trade: TradeData):
        """
        Convert trade type to buy/sell/short/cover
        """
        if trade.direction == Direction.LONG:
            if trade.offset == Offset.OPEN:
                return TradeType.BUY
            else:
                return TradeType.COVER
        else:
            if trade.offset == Offset.OPEN:
                return TradeType.SHORT
            else:
                return TradeType.SELL

    @staticmethod
    def inverse_req(req: OrderRequest):
        """"""
        req.direction = Direction.SHORT if req.direction == Direction.LONG else Direction.LONG
        return req

    @staticmethod
    def strip_digit(symbol: str):
        """"""
        res = ""
        for char in symbol:
            if not char.isdigit():
                res += char
            else:
                break
        return res

    def is_intra_day_symbol(self, symbol: str):
        """"""
        return self.strip_digit(symbol) in self.intraday_symbols

    def split_req(self, req: OrderRequest):
        """
        Split order to max signle order limit
        """
        symbol = self.strip_digit(req.symbol)
        symbol_single_max = self.single_max_dict.get(symbol, self.single_max)
        order_max = min(symbol_single_max, self.single_max)

        if req.volume <= order_max:
            return [req]

        max_count, remainder = divmod(req.volume, order_max)

        req_max = copy(req)
        req_max.volume = order_max
        req_list = [req_max for i in range(int(max_count))]

        if remainder:
            req_r = copy(req)
            req_r.volume = remainder
            req_list.append(req_r)
        return req_list

    def register_event(self):
        """"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_FOLLOW_ORDER, self.process_follow_order_event)
        self.event_engine.register(EVENT_FOLLOW_MODIFY_POS, self.process_follow_modify_pos_event)

    def process_tick_event(self, event: Event):
        """"""
        tick = event.data
        self.tick_time = tick.datetime
        self.init_limited_price(tick)
        self.update_latest_price(tick)

    def is_duplicated_order(self, order: OrderData):
        if order.vt_orderid in self.vt_accepted_orderids:
            # 若源户已处理的委托全部成交，并且此单已经成功提交跟单到交易所，才允许追单计时
            # 因为本次回报ID之前已登记过了，所以委托成交后，需要在检查去重之前就开始做追单计时，否则会被去重功能过滤掉
            if order.status in [Status.ALLTRADED] and order.vt_orderid in self.tradeid_orderids_dict:
                # 如果一开始就全部成交的（推送2个委托回报），因为不在保留委托名单里面，所以第二个推送的委托在这里不会触发以下代码
                if order.vt_orderid in self.orderid_keep_hang:
                    self.orderid_keep_hang.remove(order.vt_orderid)
                    self.write_log(f"委托单{order.vt_orderid}全部成交，从保留委托队列中移除，开始追单计时……")

                    # 开始追单
                    for vt_orderid in self.get_follow_orderids(order.vt_orderid):
                        # 委托模式下，超价会导致信号户未成交而跟单户成交的状况，要先判断是否成交
                        order_ = self.main_engine.get_order(vt_orderid)
                        # order_偶尔会变成空值导致报错
                        if order_ is not None:
                            if not order_.is_active():
                                print(f"{vt_orderid}已经不是活动委托")
                                continue

                        # 开始计时
                        self.active_order_set.add(vt_orderid)
                        self.active_order_counter[vt_orderid] = 0
                        self.cancel_counter[vt_orderid] = 0

            return True
        else:
            self.vt_accepted_orderids.add(order.vt_orderid)
            return False

    def process_order_event(self, event: Event):
        """
        process order from target gateway.
        """
        try:
            order = event.data
            vt_orderid = order.vt_orderid

            if order.gateway_name == self.source_gateway_name:

                if self.follow_based == FollowBaseMode.BASE_TRADE:
                    return

                # 委托已提交
                # 一般三种情况：挂单、直接部分成交，直接全部成交
                if order.status in [Status.NOTTRADED, Status.PARTTRADED, Status.ALLTRADED]:
                    # 委托回报会有多笔，必须对重复回报过滤，否则会触发多次跟单，但是此处对已撤销状态的回报不做重复过滤
                    # 过滤重复推送
                    if self.is_duplicated_order(order):
                        return

                    if not self.is_active:
                        self.write_log(f"委托单{order.vt_orderid}不跟随，系统尚未启动。")
                        return

                    # 功能过滤
                    if not self.filter_source_order(order):
                        return

                    self.write_log(f"委托单{order.vt_orderid}核验通过，执行跟随。")

                    # 核验成功
                    # 跟随委托模式暂不支持开平转换和日内交易开平计算，直接发单
                    req = self.convert_order_to_order_req(order)
                    self.send_order(req, order.vt_orderid, is_must_done=True)

                # 跟单系统追单是通过计算活动委托的时间来实现的，所以只要是活动委托，跟单系统就会开始计时，所以如果要挂单，就必须做额外的标记
                # 若源户委托未成交（挂单）或部分成交，则该委托加入保留队列，跟单户通过这个名单来判断是否会被撤单
                # 若源户直接全部成交，则不加入保留名单，处理跟单户的委托时就会做追单计时
                if order.status in [Status.NOTTRADED, Status.PARTTRADED]:
                    print(f"{order.vt_orderid}已加入保留委托列表")
                    self.orderid_keep_hang.add(order.vt_orderid)

                # 源户主动撤单
                if order.status == Status.CANCELLED:
                    # 源户主动撤单，先把委托单从追单列表中移除
                    order_ids = self.get_follow_orderids(order.vt_orderid)
                    for order_id in order_ids:
                        # 显式指出撤单后不允许追单
                        self.cancel_order(order_id, is_allow_resend=False)

            # 处理跟单户委托
            if order.gateway_name == self.target_gateway_name:
                # 更新offset converter
                self.offset_converter.update_order(order)

                # 过滤与跟随系统无关的委托
                if not self.filter_target_not_follow(order.vt_orderid):
                    # self.write_log(f"{order.vt_orderid}不是跟随策略产生的委托。")
                    return

                # 追单后的保留委托不做撤单处理
                if order.vt_orderid in self.fail_chase_orderid:
                    return

                if order.is_active():
                    # 委托模式下，若允许保留委托单，则不做追单计时
                    if self.follow_based == FollowBaseMode.BASE_ORDER:
                        signal_orderid = self.orderid_to_signal_orderid.get(order.vt_orderid)
                        if signal_orderid and signal_orderid in self.orderid_keep_hang:
                            print("属于保留委托，不执行撤单超时计算")
                            return

                    # 非保留委托单（源户成交），开始做追单计时
                    self.active_order_set.add(vt_orderid)
                    self.active_order_counter[vt_orderid] = 0
                    self.cancel_counter[vt_orderid] = 0
                else:
                    if vt_orderid in self.active_order_set:
                        self.active_order_counter.pop(vt_orderid)
                        self.active_order_set.remove(vt_orderid)

                    # 追单逻辑
                    if order.status == Status.CANCELLED:
                        # 没有跟随成功的单子记录到丢失单
                        if vt_orderid in self.open_orderids:
                            self.add_lost_follow(order)

                        # 如果开启追单则重新发单
                        if vt_orderid in self.chase_orderids:
                            ancestor_orderid = self.chase_ancestor_dict.get(vt_orderid)
                            resend_count = self.chase_resend_count_dict.get(ancestor_orderid)
                            if resend_count < self.chase_max_resend:
                                self.resend_order(order, self.chase_base_last_order_price)
                            else:
                                self.write_log(f"原始委托{ancestor_orderid}超过最大追单次数。")

                                if self.is_keep_order_after_chase:
                                    # 追单失败后发送新委托保留订单，并把id加入专用容器，用于手动同步定向撤单
                                    new_orderid = self.direct_send_base_order(order)
                                    self.fail_chase_orderid.add(new_orderid)
                                    self.write_log(f"原始委托{ancestor_orderid}追单失败后直接发送新委托以做保留。")
        except:  # noqa
            msg = f"处理委托事件，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def process_trade_event(self, event: Event):
        """"""
        try:
            trade = event.data

            # 断线重连，过滤重复推送的成交
            if trade.vt_tradeid in self.vt_tradeids:
                self.write_log(f"{trade.vt_tradeid}是重复推送。")
                return
            else:
                self.vt_tradeids.add(trade.vt_tradeid)

            if trade.gateway_name == self.source_gateway_name:
                # 更新源户持仓并刷新UI
                self.update_source_pos_by_trade(trade)

                # 此过滤放前面，不然在委托模式未启动前会收到“系统未启动成交不跟随”的提示，容易引起错误理解
                if self.follow_based == FollowBaseMode.BASE_ORDER:
                    return

                # 核验源户的成交单
                if not self.is_active:
                    self.write_log(f"成交单{trade.vt_tradeid}不跟随，系统尚未启动。")
                    return

                if not self.filter_source_trade(trade):
                    return

                # 将原始成交单进行平今平仓拆分
                if not self.is_intraday_trading:
                    trade_dict = self.get_trade_dict(trade, True)
                    trades = [trade_dict]
                else:
                    trades = self.split_trade_to_open_close(trade)

                    # 更新源户成交头寸，刷新UI并保存
                    self.update_source_traded_net(trade.vt_symbol, self.get_trade_net_vol(trade))
                    self.save_follow_data()

                self.write_log(f"成交单{trade.vt_tradeid}核验通过，执行跟随。")

                # 处理核验通过的成交
                for trade_dict in trades:
                    trade = trade_dict['trade']
                    is_must_done = trade_dict['is_must_done']
                    # print(trade.vt_tradeid, 'must_done:', is_must_done)

                    # 生成基于成交单的发单请求
                    req = self.convert_trade_to_order_req(trade, is_must_done)
                    if not req:
                        continue

                    # 将订单压入待处理列表，由事件引擎执行实际发单，防止线程冲突
                    self.send_order(req, trade.vt_tradeid, is_must_done)
            else:
                self.offset_converter.update_trade(trade)
                self.update_target_pos_by_trade(trade)

                if not self.filter_target_not_follow(trade.vt_orderid):
                    self.write_log(f"{trade.vt_tradeid} 不是跟随策略的成交单。")
                    return

                self.save_follow_data()
                self.write_log(f"{trade.vt_symbol}仓位更新成功。")

        except:  # noqa
            msg = f"处理成交事件，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def process_timer_event(self, event: Event):
        """"""
        try:
            self.send_queue_order()
            self.cancel_timeout_order()
            self.auto_save_trade()
        except:  # noqa
            msg = f"处理定时事件，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def process_follow_order_event(self, event: Event):
        """"""
        try:
            req, vt_tradeid, is_must_done = event.data
            self.send_and_record(req, vt_tradeid, is_must_done)
        except:
            msg = f"处理FollowOrder事件，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def process_follow_modify_pos_event(self, event: Event):
        try:
            vt_symbol, modify_pos_dict = event.data

            self.set_pos(vt_symbol, 'basic_delta', modify_pos_dict['basic_delta'])
            self.set_pos(vt_symbol, 'source_traded_net', modify_pos_dict['source_traded_net'])
            self.set_pos(vt_symbol, 'lost_follow_net', modify_pos_dict['lost_follow_net'])

            self.put_pos_delta_event(vt_symbol)
            self.save_follow_data()
            self.write_log(f"{vt_symbol}仓位修改成功")
        except:
            msg = f"处理FollowModifyPos事件，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def process_position_event(self, event: Event):
        """
        update source gateway position and target gateway offset converter position
        """
        try:
            position = event.data

            if self.is_active:
                self.pre_subscribe(position)

            # 更新源户和目标户持仓，刷新UI
            # 当某个合约的仓位为0时，接口可能不会推送这个合约的数据，因此会导致测试环境下，仓位计算有可能不准确
            # 但是只要有新的成交即可恢复正常。
            if position.gateway_name == self.source_gateway_name:
                self.update_source_pos_by_pos(position)
            else:
                self.offset_converter.update_position(position)
                self.update_target_pos_by_pos(position)
        except:  # noqa
            msg = f"处理持仓事件，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def add_lost_follow(self, order: OrderData):
        """"""
        symbol_pos = self.get_symbol_pos(order.vt_symbol)
        if order.direction == Direction.LONG:
            lost_vol = order.volume - order.traded
        else:
            lost_vol = -(order.volume - order.traded)
        symbol_pos['lost_follow_net'] += lost_vol

        self.put_pos_delta_event(order.vt_symbol)
        self.save_follow_data()

    def split_trade_to_open_close(self, trade: TradeData):
        """
        split trade to open or close by source gateway traded net pos.
        """
        symbol_pos = self.get_symbol_pos(trade.vt_symbol)
        source_traded_net = symbol_pos['source_traded_net']

        trades = []
        trade_net_vol = self.get_trade_net_vol(trade)
        if source_traded_net == 0:
            trades.append(self.get_trade_dict(trade, False))
        elif source_traded_net > 0:
            if trade_net_vol > 0:
                trades.append(self.get_trade_dict(trade, False))
            else:
                if abs(trade_net_vol) <= source_traded_net:
                    trades.append(self.get_trade_dict(trade, True))
                else:
                    close_trade = copy(trade)
                    close_trade.volume = source_traded_net
                    trades.append(self.get_trade_dict(close_trade, True))
                    open_trade = copy(trade)
                    open_trade.volume = abs(trade_net_vol + source_traded_net)
                    trades.append(self.get_trade_dict(open_trade, False))
        else:
            if trade_net_vol < 0:
                trades.append(self.get_trade_dict(trade, False))
            else:
                if trade_net_vol <= abs(source_traded_net):
                    trades.append(self.get_trade_dict(trade, True))
                else:
                    close_trade = copy(trade)
                    close_trade.volume = abs(source_traded_net)
                    trades.append(self.get_trade_dict(close_trade, True))
                    open_trade = copy(trade)
                    open_trade.volume = abs(trade_net_vol + source_traded_net)
                    trades.append(self.get_trade_dict(open_trade, False))
        return trades

    def pre_subscribe(self, position: PositionData):
        """
        Pre subscribe symbol in source gateway position to speed up following.
        """
        vt_symbol = position.vt_symbol
        if vt_symbol in self.pre_subscribe_symbols:
            return

        if not self.is_price_inited(vt_symbol):
            if self.subscribe(vt_symbol):
                self.pre_subscribe_symbols.add(vt_symbol)
                self.write_log(f"{vt_symbol}行情订阅请求已发送。")

    def cancel_timeout_order(self):
        """
        Cancel active order if timeout exceed specified value.
        """
        for vt_orderid, counter in copy(self.active_order_counter).items():
            print("counter:", vt_orderid, counter)
            if counter is None:
                continue

            # 3.6以上版本timer event差不多1秒1次，不用加乘数，旧版本大约1秒2次，要加乘数
            if vt_orderid in self.chase_orderids:
                # 追单委托中再判断一下，是否为第一笔委托
                if vt_orderid in self.first_orderids:
                    cancel_timeout = self.cancel_order_timeout
                    prefix = "普通"
                else:
                    cancel_timeout = self.chase_order_timeout
                    prefix = "追单"
            else:
                cancel_timeout = self.cancel_order_timeout
                prefix = "普通"

            cancel_counter = self.cancel_counter.get(vt_orderid, None)
            if cancel_counter and cancel_counter > self.max_cancel:
                self.write_log(f"{prefix}委托单{vt_orderid} 撤单超过{self.max_cancel}次，停止撤单。")
                self.active_order_counter.pop(vt_orderid)
                self.active_order_set.remove(vt_orderid)
                continue

            if counter > cancel_timeout:
                self.cancel_order(vt_orderid, is_allow_resend=True)
                self.active_order_counter[vt_orderid] = 0
                self.cancel_counter[vt_orderid] += 1
                self.write_log(f"{prefix}委托单{vt_orderid} 超过最大等待时间，已执行撤单。")

            self.active_order_counter[vt_orderid] += 1

    def resend_order(self, order: OrderData, base_last_order_price: bool = True):
        """"""
        new_volume = order.volume - order.traded

        if base_last_order_price:
            price = self.convert_order_price(order.vt_symbol,
                                             order.direction,
                                             price=order.price,
                                             tick_add=self.chase_order_tick_add)
        else:
            price = self.convert_order_price(order.vt_symbol,
                                             order.direction,
                                             tick_add=self.chase_order_tick_add,
                                             base_price=self.chase_base_price)

        ancestor_orderid = self.chase_ancestor_dict.get(order.vt_orderid)
        req = OrderRequest(
            symbol=order.symbol,
            exchange=order.exchange,
            direction=order.direction,
            type=OrderType.LIMIT,
            volume=new_volume,
            price=price,
            offset=order.offset,
            reference=f"{APP_NAME}_Chase"
        )

        vt_orderid = self.main_engine.send_order(req, self.target_gateway_name)
        self.chase_orderids.add(vt_orderid)
        self.chase_ancestor_dict[vt_orderid] = ancestor_orderid
        self.chase_resend_count_dict[ancestor_orderid] += 1

        self.intraday_orderids.add(vt_orderid)

    def direct_send_base_order(self, order: OrderData, price: float = None):
        if not price:
            price = order.price
        new_vol = order.volume - order.traded

        req = OrderRequest(
            symbol=order.symbol,
            exchange=order.exchange,
            direction=order.direction,
            type=OrderType.LIMIT,
            volume=new_vol,
            price=price,
            offset=order.offset,
            reference=f"{APP_NAME}_KeepChase"
        )
        return self.main_engine.send_order(req, self.target_gateway_name)

    def refresh_pos(self):
        """
        Put pos delta event regularly, Deprecited.
        """
        if self.refresh_pos_interval > 3:
            for vt_symbol in self.positions:
                self.put_pos_delta_event(vt_symbol)
            self.refresh_pos_interval = 0
        self.refresh_pos_interval += 1

    def view_pos(self):
        """
        For Test Only.
        """
        print('=' * 100)
        print('Symbol Position:')
        print('-' * 100)
        for symbol, pos_d in self.positions.items():
            print(symbol, end="\t|\t")
            for k, v in pos_d.items():
                print(k, v, end='\t')
            print('')
            print('-' * 100)

    def view_vars(self):
        """
        For Test Only.
        """
        print("对象实例属性：")
        print("#" * 50)
        for key, value in self.__dict__.items():
            if key not in ['parameters', 'variables', 'clear_variables', 'pos_key']:
                print(key, value)
                print('-' * 50)

    def view_test_variables(self):
        """
        For Test Only.
        """
        if self.test_count > 5:
            self.view_pos()
            print('vt_tradeids', self.vt_tradeids)
            self.test_count = 0
        self.test_count += 1

    def clear_empty_pos(self):
        """
        Clear empty pos data after stop engine.
        """
        for symbol in list(self.positions.keys()):
            pos = self.positions[symbol]
            if (pos['source_long'] + pos['source_short'] + pos['target_long'] + pos['target_short'] == 0):
                self.positions.pop(symbol)

    def clear_expired_pos(self):
        """
        clear expired contract data
        """
        for symbol in list(self.positions.keys()):
            contract = self.main_engine.get_contract(symbol)
            if not contract:
                self.positions.pop(symbol)
                self.write_log(f"{symbol}已过期，清除成功。")

    def get_follow_orderids(self, vt_tradeid: str):
        """"""
        if self.tradeid_orderids_dict.get(vt_tradeid) is None:
            self.tradeid_orderids_dict[vt_tradeid] = list()
        return self.tradeid_orderids_dict[vt_tradeid]

    def get_symbol_pos(self, vt_symbol: str):
        """
        Get pos by vt_symbol, if None then initial symbol pos.
        """
        if self.positions.get(vt_symbol, None) is None:
            self.init_symbol_pos(vt_symbol)
        symbol_pos = self.positions[vt_symbol]
        return symbol_pos

    def init_symbol_pos(self, vt_symbol: str):
        """
        Inital symbol pos dict.
        """
        self.positions[vt_symbol] = {}
        for pos_key in self.pos_key:
            self.positions[vt_symbol][pos_key] = 0

    def update_source_traded_net(self, vt_symbol: str, delta_vol: int):
        """
        Update source traded net by trade net vol in order to distinct open or close order.
        """
        symbol_pos = self.get_symbol_pos(vt_symbol)
        symbol_pos['source_traded_net'] += delta_vol
        self.put_pos_delta_event(vt_symbol)

    def update_source_pos_by_pos(self, position: PositionData):
        """"""
        if position.direction == Direction.NET:
            return

        symbol_pos = self.get_symbol_pos(position.vt_symbol)
        if position.direction == Direction.LONG:
            symbol_pos['source_long'] = position.volume
        else:
            symbol_pos['source_short'] = position.volume

        symbol_pos['source_net'] = symbol_pos['source_long'] - symbol_pos['source_short']
        symbol_pos['net_delta'] = symbol_pos['source_net'] * self.multiples - symbol_pos['target_net']

        self.put_pos_delta_event(position.vt_symbol)

    def update_target_pos_by_pos(self, position: PositionData):
        """"""
        if position.direction == Direction.NET:
            return

        symbol_pos = self.get_symbol_pos(position.vt_symbol)
        if position.direction == Direction.LONG:
            symbol_pos['target_long'] = position.volume
        else:
            symbol_pos['target_short'] = position.volume

        symbol_pos['target_net'] = symbol_pos['target_long'] - symbol_pos['target_short']
        symbol_pos['net_delta'] = symbol_pos['source_net'] * self.multiples - symbol_pos['target_net']

        self.put_pos_delta_event(position.vt_symbol)

    def update_source_pos_by_trade(self, trade: TradeData):
        """"""
        symbol_pos = self.get_symbol_pos(trade.vt_symbol)
        trade_type = self.get_trade_type(trade)
        if trade_type == TradeType.BUY:
            symbol_pos['source_long'] += trade.volume
        elif trade_type == TradeType.SHORT:
            symbol_pos['source_short'] += trade.volume
        elif trade_type == TradeType.SELL:
            symbol_pos['source_long'] -= trade.volume
        else:
            symbol_pos['source_short'] -= trade.volume

        symbol_pos['source_net'] = symbol_pos['source_long'] - symbol_pos['source_short']
        symbol_pos['net_delta'] = symbol_pos['source_net'] * self.multiples - symbol_pos['target_net']

        self.put_pos_delta_event(trade.vt_symbol)

    def update_target_pos_by_trade(self, trade: TradeData):
        """"""
        vt_symbol = trade.vt_symbol
        if self.positions.get(vt_symbol, None) is None:
            self.init_symbol_pos(vt_symbol)

        symbol_pos = self.positions[vt_symbol]
        trade_type = self.get_trade_type(trade)
        if trade_type == TradeType.BUY:
            symbol_pos['target_long'] += trade.volume
        elif trade_type == TradeType.SHORT:
            symbol_pos['target_short'] += trade.volume
        elif trade_type == TradeType.SELL:
            symbol_pos['target_long'] -= trade.volume
        else:
            symbol_pos['target_short'] -= trade.volume

        symbol_pos['target_net'] = symbol_pos['target_long'] - symbol_pos['target_short']
        symbol_pos['net_delta'] = symbol_pos['source_net'] * self.multiples - symbol_pos['target_net']

        self.put_pos_delta_event(trade.vt_symbol)

    def subscribe(self, vt_symbol: str):
        """
        Subscribe to get latest price and limit price.
        """
        contract = self.main_engine.get_contract(vt_symbol)
        if contract:
            req = SubscribeRequest(symbol=contract.symbol, exchange=contract.exchange)
            gateway_name = self.target_gateway_name if self.source_gateway_name == "RPC" else self.source_gateway_name
            self.main_engine.subscribe(req, gateway_name)
            return True

    def init_limited_price(self, tick: TickData):
        """
        Save symbol limit-up and limit-down price.
        """
        vt_symbol = tick.vt_symbol
        if vt_symbol not in self.limited_prices:
            d = {
                'limit_up': tick.limit_up,
                'limit_down': tick.limit_down
            }
            self.limited_prices[vt_symbol] = d

    def update_latest_price(self, tick: TickData):
        """
        Update symbol bid-1 price and ask-1 price.
        """
        vt_symbol = tick.vt_symbol
        if self.latest_prices.get(vt_symbol, None) is None:
            self.latest_prices[vt_symbol] = {}
        self.latest_prices[vt_symbol]['bid_price'] = tick.bid_price_1
        self.latest_prices[vt_symbol]['ask_price'] = tick.ask_price_1

    def is_timeout_trade(self, trade: Union[TradeData, OrderData]):
        """
        If trade happened a specified period of time before now, it usually happened if take a long time to reconnect.
        Because trade is not in self.vt_tradeids(if app don't restart). so it can't be filtered by self.vt_tradeids
        """
        now = self.get_current_time()
        trade_time = datetime.strptime(self.get_trade_time(trade), '%H:%M:%S')
        trade_time = trade_time.replace(year=now.year, month=now.month, day=now.day)

        prefix_str = "成交单" if isinstance(trade, TradeData) else "委托单"
        if now - trade_time > timedelta(seconds=self.filter_trade_timeout):
            self.write_log(f"{prefix_str}{trade.vt_tradeid} 时间：{self.get_trade_time(trade)} 超过跟单有效期。")
            return True
        else:
            return False

    def is_followed_trade(self, trade: TradeData):
        """"""
        if trade.vt_tradeid in self.tradeid_orderids_dict:
            self.write_log(f"成交单{trade.vt_tradeid} 已跟随，无需重复跟随。")
            return True
        else:
            return False

    def is_followed_order(self, order: OrderData):
        if order.vt_orderid in self.tradeid_orderids_dict:
            self.write_log(f"委托单{order.vt_orderid} 已跟随，无需重复跟随。")
            return True
        else:
            return False

    def is_skip_contract_trade(self, trade: Union[TradeData, OrderData]):
        """
        Check order or trade contract
        """
        if isinstance(trade, TradeData):
            id_string = trade.vt_tradeid
        else:
            id_string = trade.vt_orderid

        if trade.vt_symbol in self.skip_contracts:
            self.write_log(f"{id_string} 合约{trade.vt_symbol}禁止同步。")
            return True
        else:
            return False

    def is_to_follow_volume(self, trade: Union[TradeData, OrderData]):
        """
        Check order or trade volume
        """
        if self.is_filter_order_vol:
            if isinstance(trade, TradeData):
                order = self.main_engine.get_order(trade.vt_orderid)
            else:
                order = trade

            if order.volume in self.order_volumes_to_follow:
                return True
            else:
                self.write_log(f"委托单{order.vt_orderid}手数{order.volume}不符合跟单规则。")
                return False
        else:
            return True

    def filter_source_trade(self, trade: TradeData):
        """
        Filter trade from source gateway.
        """
        # 过滤手数
        if not self.is_to_follow_volume(trade):
            return

        # 过滤黑名单合约
        if self.is_skip_contract_trade(trade):
            return

        # 过滤已成交
        if self.is_followed_trade(trade):
            return

        # 过滤超时成交单（网络质量太差，很少发生）
        if self.is_timeout_trade(trade):
            return

        return trade

    def filter_source_order(self, order: OrderData):
        """
        Filter order from source gateway.
        """
        if not self.is_to_follow_volume(order):
            return

        if self.is_skip_contract_trade(order):
            return

        if self.is_followed_order(order):
            return

        if self.is_timeout_trade(order):
            return

        return order

    def filter_target_not_follow(self, vt_orderid: str):
        """"""
        if vt_orderid in self.chase_orderids:
            return True

        for sub_list in self.tradeid_orderids_dict.values():
            for orderid in sub_list:
                if vt_orderid == orderid:
                    return True

        if vt_orderid in self.fail_chase_orderid:
            return True

    def validate_target_pos(self, req: OrderRequest):
        """
        Validate symbol pos in target gateway.
        """
        vt_symbol = req.vt_symbol
        symbol_pos = self.positions.get(vt_symbol, None)
        if symbol_pos is None:
            self.write_log(f"{vt_symbol} 跟随策略该品种的仓位不存在。")
            return
        else:
            if req.direction == Direction.LONG:
                short_pos = symbol_pos['target_short']
                if short_pos <= 0:
                    self.write_log(f"{vt_symbol} 跟随策略该品种空头仓位不足。")
                    return
                else:
                    req.volume = min(req.volume, short_pos)
            else:
                long_pos = symbol_pos['target_long']
                if long_pos <= 0:
                    self.write_log(f"{vt_symbol} 跟随策略该品种多头仓位不足。")
                    return
                else:
                    req.volume = min(req.volume, long_pos)
            return req

    def convert_order_price(
        self,
        vt_symbol: str,
        direction: Direction,
        price: float = 0,
        is_must_done: bool = False,
        tick_add: Optional[int] = None,
        base_price: OrderBasePrice = OrderBasePrice.GOOD_FOR_SELF
    ):
        """
        Make sure price is in limit-up and limit-down range.
        """
        if tick_add is None:
            tick_add = self.must_done_tick_add if is_must_done else self.tick_add

        # 只有当self.is_price_inited()市场数据初始化完成才会直接运行这个函数.
        limit_price = self.limited_prices.get(vt_symbol)
        latest_prices = self.latest_prices.get(vt_symbol)
        ask_price, bid_price = latest_prices['ask_price'], latest_prices['bid_price']

        # 涨跌停的时候不能直接用latest_prices，需要做处理，有些版本涨跌停数据是显示天文数字
        if ask_price == 0:
            ask_price = limit_price['limit_up']
        else:
            # 处理旧版本的涨停
            ask_price = min(latest_prices['ask_price'], limit_price['limit_up'])

        if bid_price == 0:
            bid_price = limit_price['limit_down']
        else:
            if latest_prices['bid_price'] > limit_price['limit_up']:
                bid_price = limit_price['limit_down']
            else:
                bid_price = latest_prices['bid_price']

        print('ask:', ask_price, 'bid:', bid_price, 'price:', price)
        contract = self.main_engine.get_contract(vt_symbol)
        if direction == Direction.LONG:
            if not price:
                price = ask_price if base_price == OrderBasePrice.GOOD_FOR_OTHER else bid_price
            # 用price设置为-1来表示手动模式下的市价发单
            if self.order_type == OrderType.MARKET or price == -1:
                price = limit_price['limit_up']
            else:
                price = min(limit_price['limit_up'], price + tick_add * contract.pricetick)
        else:
            if not price:
                price = bid_price if base_price == OrderBasePrice.GOOD_FOR_OTHER else ask_price
            if self.order_type == OrderType.MARKET or price == -1:
                price = limit_price['limit_down']
            else:
                price = max(limit_price['limit_down'], price - tick_add * contract.pricetick)

        return price

    def convert_order_to_order_req(self, order: OrderData):
        """
        Order converter to order request. Only support convert directly, do not support intraday mode.
        """
        req = OrderRequest(
            symbol=order.symbol,
            exchange=order.exchange,
            direction=order.direction,
            type=OrderType.LIMIT,
            volume=order.volume,
            price=order.price,
            offset=order.offset,
            reference=f"{APP_NAME}_OrderMod"
        )
        req.volume = req.volume * self.multiples
        if self.inverse_follow:
            req = self.inverse_req(req)
        return req

    def convert_trade_to_order_req(self, trade: TradeData, is_must_done: bool = False):
        """
        Trade convert to order request
        """
        if trade.offset == Offset.NONE:
            self.write_log(f"{trade.vt_tradeid} offset为None，非CTP正常成交单。")
            return
        if trade.direction == Direction.NET:
            self.write_log(f"{trade.vt_tradeid} direction为Net， 非CTP正常成交单。")
            return

        vt_symbol = trade.vt_symbol
        req = OrderRequest(
            symbol=trade.symbol,
            exchange=trade.exchange,
            direction=trade.direction,
            type=OrderType.LIMIT,
            volume=trade.volume,
            price=trade.price,
            offset=trade.offset,
            reference=f"{APP_NAME}_TradeMod"
        )
        req_net_vol = self.get_req_net_vol(req) * self.multiples
        req.volume = req.volume * self.multiples

        if self.inverse_follow:
            req = self.inverse_req(req)

        # 检查丢失仓位
        if self.is_intraday_trading and is_must_done:
            symbol_pos = self.get_symbol_pos(vt_symbol)
            lost_folow_vol = symbol_pos['lost_follow_net']
            if lost_folow_vol != 0:
                if req.volume > abs(lost_folow_vol):
                    # 更新丢失净头寸时需要先计算
                    to_close_vol = symbol_pos['lost_follow_net'] + req_net_vol

                    symbol_pos['lost_follow_net'] = 0
                    self.put_pos_delta_event(vt_symbol)

                    req.volume = abs(to_close_vol)
                else:
                    symbol_pos['lost_follow_net'] += req_net_vol
                    self.put_pos_delta_event(vt_symbol)
                    # It will not follow trade, so save data here
                    self.save_follow_data()

                    self.write_log(f"{vt_symbol}丢失净仓：{lost_folow_vol}, 平仓净仓：{req_net_vol}, 无需跟随日内平仓。")
                    return

        # 日内合约使用锁仓模式（避免平今），转处理
        if self.strip_digit(vt_symbol) in self.intraday_symbols:
            return req

        # 普通模式
        if trade.offset != Offset.OPEN:
            req.offset = Offset.CLOSE
            return self.validate_target_pos(req)
        else:
            return req

    def is_price_inited(self, vt_symbol: str):
        """
        Check if limited price and latest price ready.
        """
        if self.limited_prices.get(vt_symbol, None) and self.latest_prices.get(vt_symbol, None):
            return True
        else:
            return False

    def send_order(
        self,
        req: OrderRequest,
        vt_tradeid: str,
        is_must_done: bool = False
    ):
        """
        Send order to order queue.
        """
        if not self.is_price_inited(req.vt_symbol):
            self.subscribe(req.vt_symbol)
            self.write_log(f"{req.vt_symbol}订阅请求已发送。")

            # 把委托任务压入任务列表
            self.due_out_req_list.append((vt_tradeid, req, is_must_done))
        else:
            # 直接向事件引擎压入发单事件
            order_tuple = (req, vt_tradeid, is_must_done)
            self.put_follow_order_event(order_tuple)

    def send_queue_order(self):
        """
        Send order in queue after limited price is ready.
        """
        if not self.due_out_req_list:
            return

        for req_tuple in copy(self.due_out_req_list):
            vt_tradeid, req, is_must_done = req_tuple
            if not self.is_price_inited(req.vt_symbol):
                continue

            self.send_and_record(req, vt_tradeid, is_must_done)
            self.due_out_req_list.remove(req_tuple)

    def send_and_record(
        self,
        req: OrderRequest,
        vt_tradeid: str,
        is_must_done: bool = False
    ):
        """
        Send and record result.
        """
        if vt_tradeid.startswith("SYNC"):
            price_base = self.sync_base_price
            is_must_done = True
        else:
            price_base = OrderBasePrice.GOOD_FOR_SELF

        req.price = self.convert_order_price(req.vt_symbol, req.direction, req.price, is_must_done, base_price=price_base)
        vt_orderids = self.convert_and_send_orders(req, is_must_done)
        if vt_orderids:
            orderids_list = self.get_follow_orderids(vt_tradeid)
            orderids_list.extend(vt_orderids)

            for orderid in copy(orderids_list):
                self.orderid_to_signal_orderid[orderid] = vt_tradeid
                # 把初始跟随单加入初始set，用来区分普通超时或追单超时
                self.first_orderids.add(orderid)

            if vt_tradeid.startswith('SYNC'):
                order_prefix = "同步单"
                self.intraday_orderids.update(vt_orderids)
            elif vt_tradeid.startswith('BASIC'):
                order_prefix = "底仓单"
            else:
                order_prefix = "跟随单"
                if self.is_intraday_trading:
                    self.intraday_orderids.update(vt_orderids)

            self.write_log(f"{order_prefix} {vt_tradeid}发单成功，委托号：{'  '.join(vt_orderids)}。")
            self.save_follow_data()

        return vt_orderids

    def convert_and_send_orders(self, req: OrderRequest, is_must_done: bool = False):
        """
        Convert a req to req list and send order to gateway.
        """
        lock = True if self.strip_digit(req.vt_symbol) in self.intraday_symbols else False

        req_list = self.offset_converter.convert_order_request(req, lock=lock)
        if not req_list:
            self.write_log("委托单转换模块转换失败，可能是目标账户实际可用仓位不足。")
            return

        vt_orderids = []
        for req in req_list:
            # split req
            splited_req_list = self.split_req(req)
            for splited_req in splited_req_list:
                vt_orderid = self.main_engine.send_order(splited_req, self.target_gateway_name)
                if not vt_orderid:
                    continue
                vt_orderids.append(vt_orderid)

                if not is_must_done:
                    self.open_orderids.add(vt_orderid)

                if is_must_done and self.is_chase_order:
                    # 如果是委托模式，跟随源户主动撤单的，则不应该追单，收到信号就移除出chase_orderids
                    # 如果是软件停止或手动同步主动撤单的，也不应该追单。
                    self.chase_orderids.add(vt_orderid)
                    self.chase_ancestor_dict[vt_orderid] = vt_orderid
                    self.chase_resend_count_dict[vt_orderid] = 0
                self.offset_converter.update_order_request(splited_req, vt_orderid)

        return vt_orderids

    def cancel_order(self, vt_orderid: str, is_allow_resend: bool = False):
        """
        Cancel existing order by vt_orderid.
        """
        order = self.main_engine.get_order(vt_orderid)
        if not order:
            self.write_log(f"撤单失败，找不到委托号 {vt_orderid}。")
            return

        # 若是人工撤单或手动同步前撤单，表示要开始人工干预，此单撤销后不允许再追单
        if not is_allow_resend:
            if vt_orderid in self.chase_orderids:
                self.chase_orderids.remove(vt_orderid)

        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)
        self.write_log(f"委托号{vt_orderid}撤单请求已报。")

    def cancel_all_order(self, vt_symbol: str = "", is_allow_resend: bool = False, is_only_fail_chase: bool = False):
        """
        Cancel all active orders or orders of vt_symbol in target gateway and generated by this app.
        """
        active_orders = self.main_engine.get_all_active_orders(vt_symbol)

        target_orders = [order for order in active_orders if order.gateway_name == self.target_gateway_name]
        for order in target_orders:
            # 过滤掉非追单系统下的委托
            if not self.filter_target_not_follow(order.vt_orderid):
                continue

            # is_only_fail_chase设为True只允许撤追单失败后发出新委托做保留之用的那条委托
            if is_only_fail_chase:
                if order.vt_orderid not in self.fail_chase_orderid:
                    continue

            # is_allow_resend设为true允许此撤单引发追单
            self.cancel_order(order.vt_orderid, is_allow_resend)

    def is_pos_exists(self, vt_symbol: str):
        """
        If pos of vt_symbol in positions
        """
        symbol_pos = self.positions.get(vt_symbol, None)
        if symbol_pos is None:
            self.write_log(f"{vt_symbol}仓位不存在。")
            return False
        else:
            return True

    def get_pos_delta(self, vt_symbol: str):
        """
        Calculate pos delta between source gateway and target gateway. make sure vt_symbol is existed.
        """
        # 计算持仓差
        symbol_pos = self.positions.get(vt_symbol, None)

        if not self.inverse_follow:
            long_pos_delta = symbol_pos['source_long'] * self.multiples - symbol_pos['target_long']
            short_pos_delta = symbol_pos['source_short'] * self.multiples - symbol_pos['target_short']
        else:
            long_pos_delta = symbol_pos['source_short'] * self.multiples - symbol_pos['target_long']
            short_pos_delta = symbol_pos['source_long'] * self.multiples - symbol_pos['target_short']

        return long_pos_delta, short_pos_delta

    def get_net_pos_delta(self, vt_symbol: str):
        """
        Calculate net pos. If not sync basic position, it need adjust by basic pos.
        """
        symbol_pos = self.positions.get(vt_symbol, None)
        delta = symbol_pos['source_net'] * self.multiples - symbol_pos['target_net']
        net_pos_delta = delta if not self.inverse_follow else (- delta)
        return net_pos_delta

    def sync_net_pos_delta(self, vt_symbol: str, is_sync_basic: bool = False):
        """
        If contract is intra-day mode. Only can sync by net pos.
        """
        if self.strip_digit(vt_symbol) in self.intraday_symbols:
            symbol_pos = self.positions.get(vt_symbol, None)
            net_pos_delta = self.get_net_pos_delta(vt_symbol)
            if not is_sync_basic:
                net_pos_delta = net_pos_delta - symbol_pos['basic_delta']

            market_price = True if is_sync_basic else False
            if net_pos_delta > 0:
                self.buy(vt_symbol, net_pos_delta, market_price=market_price, is_basic=is_sync_basic)
            elif net_pos_delta < 0:
                self.short(vt_symbol, abs(net_pos_delta), market_price=market_price, is_basic=is_sync_basic)
            else:
                self.write_log(f"{vt_symbol}净仓差与底仓差一致，仓差不是跟随交易引起的，无需同步。")
                return

            if is_sync_basic:
                symbol_pos = self.positions.get(vt_symbol, None)
                symbol_pos['basic_delta'] = 0
        else:
            self.write_log(f"{vt_symbol}不是日内模式。")

    def sync_open_pos(self, vt_symbol: str):
        """"""
        if self.strip_digit(vt_symbol) in self.intraday_symbols:
            self.write_log(f"{vt_symbol}是日内模式，只支持同步净仓。")
            return

        if self.is_pos_exists(vt_symbol):
            # 在追单后保留委托的设置下，仅做定向撤单
            self.cancel_all_order(vt_symbol, is_only_fail_chase=True)

            long_pos_delta, short_pos_delta = self.get_pos_delta(vt_symbol)
            if long_pos_delta > 0:
                self.buy(vt_symbol, long_pos_delta)
                self.write_log(f"{vt_symbol}买入开仓同步指令已发出")
            else:
                # self.write_log(f"多开仓同步：{vt_symbol}目标户无仓差或多仓更多，无需同步。")
                pass

            if short_pos_delta > 0:
                self.short(vt_symbol, short_pos_delta)
                self.write_log(f"{vt_symbol}卖出开仓同步指令已发出")
            else:
                # self.write_log(f"空开仓同步：{vt_symbol}目标户无仓差或空仓更多，无需同步。")
                pass

    def sync_close_pos(self, vt_symbol: str):
        """"""
        if self.strip_digit(vt_symbol) in self.intraday_symbols:
            self.write_log(f"{vt_symbol}是日内模式，只支持同步净仓。")
            return

        if self.is_pos_exists(vt_symbol):
            self.cancel_all_order(vt_symbol, is_only_fail_chase=True)

            long_pos_delta, short_pos_delta = self.get_pos_delta(vt_symbol)
            if long_pos_delta < 0:
                self.sell(vt_symbol, abs(long_pos_delta))
                self.write_log(f"{vt_symbol}卖出平仓同步指令已发出")
            else:
                # self.write_log(f"空平仓同步：{vt_symbol}目标户无仓差或多仓更少，无需同步。")
                pass

            if short_pos_delta < 0:
                self.cover(vt_symbol, abs(short_pos_delta))
                self.write_log(f"{vt_symbol}买入平仓同步指令已发出")
            else:
                # self.write_log(f"多平仓同步：{vt_symbol}目标户无仓差或空仓更少，无需同步。")
                pass

    def sync_pos(self, vt_symbol: str):
        """Sync position between source and target by vt_symbol"""
        if self.is_pos_exists(vt_symbol):
            long_pos_delta, short_pos_delta = self.get_pos_delta(vt_symbol)
            if long_pos_delta == short_pos_delta == 0:
                self.write_log(f"{vt_symbol}源账户与目标户仓位一致，无需同步。")
                return

            self.sync_open_pos(vt_symbol)
            self.sync_close_pos(vt_symbol)

    def sync_all_pos(self):
        """Sync pos of all non-empty contract"""
        for vt_symbol in list(self.positions.keys()):
            if vt_symbol not in self.skip_contracts:
                self.sync_pos(vt_symbol)

    def send_sync_order_req(
        self,
        vt_symbol: str,
        direction: Direction,
        volume: int,
        price: float,
        offset: Offset,
        market_price: bool,
        is_basic: bool
    ):
        """
        Create order request for sync pos.
        """
        if not self.is_active:
            self.write_log("跟随系统尚未启动，不能同步。")

        contract = self.main_engine.get_contract(vt_symbol)
        req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            type=OrderType.LIMIT,
            volume=volume,
            price=price,
            offset=offset,
            reference=f"{APP_NAME}_Sync"
        )

        if market_price:
            req.price = -1

        sync_flag = "BASIC" if is_basic else "SYNC"

        # 如果没有把trade_id加入系统的trade容器，会被认为非跟随系统的单子，从而无法正确计算仓位
        now_time = self.get_current_time()
        time_id = f"{now_time.strftime('%H%M%S')}{str(now_time.microsecond // 1000)}"
        self.sync_order_ref += 1
        vt_tradeid = f"{sync_flag}_{time_id}_{self.sync_order_ref}"
        self.send_order(req, vt_tradeid)

    def buy(
        self,
        vt_symbol: str,
        volume: int,
        price: float = 0,
        market_price: bool = False,
        is_basic: bool = False
    ):
        """"""
        self.send_sync_order_req(vt_symbol, Direction.LONG, volume, price, Offset.OPEN, market_price, is_basic)

    def short(
        self,
        vt_symbol: str,
        volume: int,
        price: float = 0,
        market_price: bool = False,
        is_basic: bool = False
    ):
        """"""
        self.send_sync_order_req(vt_symbol, Direction.SHORT, volume, price, Offset.OPEN, market_price, is_basic)

    def sell(
        self,
        vt_symbol: str,
        volume: int,
        price: float = 0,
        market_price: bool = False,
        is_basic: bool = False
    ):
        """"""
        self.send_sync_order_req(vt_symbol, Direction.SHORT, volume, price, Offset.CLOSE, market_price, is_basic)

    def cover(
        self,
        vt_symbol: str,
        volume: int,
        price: float = 0,
        market_price: bool = False,
        is_basic: bool = False
    ):
        """"""
        self.send_sync_order_req(vt_symbol, Direction.LONG, volume, price, Offset.CLOSE, market_price, is_basic)

    def close_hedged_pos(self, vt_symbol: str, pos: int):
        """
        Close hedged pos.
        """
        symbol_pos = self.positions.get(vt_symbol, None)
        if symbol_pos:
            avaiable = min(symbol_pos['target_long'], symbol_pos['target_short'])
            if pos <= avaiable:
                self.sell(vt_symbol, pos, market_price=True)
                self.cover(vt_symbol, pos, market_price=True)
                self.write_log(f"已对冲仓位平仓委托单已报，{vt_symbol}，手数：{pos}。")
            else:
                self.write_log(f"平仓手数超出最大已对冲仓位。")

    def put_pos_delta_event(self, vt_symbol: str):
        """
        Calculate delta pos and put event
        """
        pos_dict = self.positions.get(vt_symbol, None)
        if pos_dict:
            pos_dict['target_net'] = pos_dict['target_long'] - pos_dict['target_short']
            pos_dict = copy(pos_dict)
            pos_dict['vt_symbol'] = vt_symbol
            pos_dict['long_delta'], pos_dict['short_delta'] = self.get_pos_delta(vt_symbol)
            pos_dict['net_delta'] = self.get_net_pos_delta(vt_symbol)

            pos_data = PosDeltaData()
            pos_data.__dict__ = pos_dict
            event = Event(EVENT_FOLLOW_POS_DELTA, pos_data)
            self.event_engine.put(event)

    def put_follow_order_event(self, order_tuple: tuple):
        """"""
        event = Event(EVENT_FOLLOW_ORDER, order_tuple)
        self.event_engine.put(event)

    def write_log(self, msg: str):
        """"""
        log = LogData(msg=msg, gateway_name=APP_NAME)
        event = Event(EVENT_FOLLOW_LOG, log)
        self.event_engine.put(event)
