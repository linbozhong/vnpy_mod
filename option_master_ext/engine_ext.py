import traceback
import typing
from typing import Optional, Dict, List, Set, Callable, Tuple
from copy import copy
from enum import Enum
from datetime import datetime

from vnpy.event import Event, EventEngine
from vnpy.trader.event import (
    EVENT_TIMER, EVENT_ORDER
)
from vnpy.trader.constant import (
    Status, Direction, Offset
)
from vnpy.trader.object import (
    BaseData, OrderData, LogData,
    OrderRequest, OrderType
)
from vnpy.trader.utility import load_json, save_json
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.app.option_master.engine import OptionEngine
from vnpy.app.option_master.base import (
    CHAIN_UNDERLYING_MAP,
    OptionData, PortfolioData, UnderlyingData, ChainData
)

APP_NAME = "OptionMasterExt"

EVENT_OPTION_STRATEGY_ORDER = "eOptionStrategyOrder"
EVENT_OPTION_HEDGE_ALGO_STATUS = "eOptionHedgeAlgoStatus"
EVENT_OPTION_HEDGE_ALGO_LOG = "eOptionHedgeAlgoLog"

STRATEGY_HEDGE_DELTA_LONG = "hedge_delta_long"
STRATEGY_HEDGE_DELTA_SHORT = "hedge_delta_short"
STRATEGY_HEDGE_DELTA_SHORT = "hedge_delta_short"
STRATEGY_STRADDLE_LONG = "straddle_long"
STRATEGY_STRADDLE_SHORT = "straddle_short"
STRATEGY_STRANGLE_LONG_ONE = "strangle_long_1"
STRATEGY_STRANGLE_SHORT_ONE = "strangle_short_1"
STRATEGY_STRANGLE_LONG_TWO = "strangle_long_2"
STRATEGY_STRANGLE_SHORT_TWO = "strangle_short_2"
STRATEGY_STRANGLE_LONG_THREE = "strangle_long_3"
STRATEGY_STRANGLE_SHORT_THREE = "strangle_short_3"


class OptionStrategy(Enum):
    SYNTHESIS = "合成"
    STRADDLE = "跨式"
    STRANGLE = "宽跨式"

class HedgeStatus(Enum):
    NOTSTART = "未启动"
    RUNNING = "监测中"
    HEDGING = "对冲中"

class StrategyOrderStatus(Enum):
    SUBMITTING = "提交中"
    SENDED = "已发送"
    FINISHED = "已完成"


class OptionStrategyOrder:

    status: StrategyOrderStatus = StrategyOrderStatus.SUBMITTING
    reqs: List[OrderRequest] = []
    active_orderids: Set[str] = set()
    time: str = ""

    def __init__(
        self,
        chain_symbol: str,
        strategy_name: OptionStrategy,
        direction: Direction,
        strategy_ref: int,
        send_at_break: bool
    ):
        self.chain_symbol = chain_symbol
        self.strategy_name = strategy_name
        self.direction = direction
        self.strategy_ref = strategy_ref
        self.send_at_break = send_at_break

        self.strategy_id = f"{self.strategy_name.value}.{self.direction.value}.{self.strategy_ref}"
        self.time = datetime.now().strftime("%H:%M:%S")

        self.status: StrategyOrderStatus = StrategyOrderStatus.SUBMITTING
        self.reqs: List[OrderRequest] = []
        self.active_orderids: Set[str] = set()

    def is_finished(self) -> bool:
        return self.status == StrategyOrderStatus.FINISHED

    def is_active(self) -> bool:
        return self.status == StrategyOrderStatus.SENDED

    def add_req(self, req: OrderRequest):
        self.reqs.append(req)


class OptionEngineExt(OptionEngine):
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__(main_engine, event_engine)
        self.engine_name = APP_NAME

        self.inited: bool = False

        self.strategy_trader: "StrategyTrader" = StrategyTrader(self)
        self.hedge_engine: "HedgeEngine" = HedgeEngine(self)

        # order funcitons
        self.buy: Optional[Callable] = None
        self.sell: Optional[Callable] = None
        self.short: Optional[Callable] = None
        self.cover: Optional[Callable] = None
        self.send_strategy_order: Optional[Callable] = None

        self.add_order_function()

    def add_order_function(self) -> None:
        self.buy = self.strategy_trader.buy
        self.sell = self.strategy_trader.sell
        self.short = self.strategy_trader.short
        self.cover = self.strategy_trader.cover
        self.send_strategy_order = self.strategy_trader.send_strategy_order

    def init_engine(self) -> None:
        self.load_portfolio_settings()
        self.init_all_portfolios()
        self.inited = True

    def load_portfolio_settings(self) -> None:
        portfolio_settings = self.setting['portfolio_settings']
        for name, settings in portfolio_settings.items():
            self.update_portfolio_setting(
                name,
                settings['model_name'],
                settings['interest_rate'],
                settings['chain_underlying_map'],
                settings['inverse'],
                settings['precision']
            )

    def init_all_portfolios(self) -> None:
        portfolio_settings = self.setting['portfolio_settings']
        for portfolio_name in portfolio_settings:
            self.init_portfolio(portfolio_name)
        

class HedgeEngine:

    setting_filename = "channel_hedge_algo_setting.json"
    data_filename = "channel_hedge_algo_data.json"

    def __init__(self, option_engine: OptionEngineExt):
        self.option_engine: OptionEngineExt = option_engine
        self.main_engine: MainEngine = option_engine.main_engine
        self.event_engine: EventEngine = option_engine.event_engine
        self.strategy_trader: "StrategyTrader" = option_engine.strategy_trader

        # parameters
        self.check_delta_trigger: int = 5
        self.calc_balance_trigger: int = 10
        self.offset_percent: float = 0.0
        self.hedge_percent: float = 0.0

        # variables
        self.chains: Dict[str, ChainData] = {}
        self.hedge_algos: Dict[str, "ChannelHedgeAlgo"] = {}
        self.counters: Dict[str, float] = {}
        self.data: Dict[str, Dict] = {}
        self.settings: Dict[str, Dict] = {}

    def load_setting(self) -> None:
        settings = load_json(self.setting_filename)
        for algo in self.hedge_algos.values():
            algo_setting = settings.get(algo.chain_symbol)
            if algo_setting:
                algo_setting.offset_percent = algo_setting['offset_percent']
                algo_setting.hedge_percent = algo_setting['hedge_percent']
        self.settings = settings
        self.write_log(f"期权对冲引擎配置载入成功")

    def save_setting(self) -> None:
        for algo in self.hedge_algos.values():
            d = {}
            d['offset_percent'] = algo.offset_percent
            d['hedge_percent'] = algo.hedge_percent
            self.settings[algo.chain_symbol] = d
        save_json(self.setting_filename, self.settings)
        self.write_log(f"期权对冲引擎配置载入成功")

    def load_data(self) -> None:
        data = load_json(self.data_filename)
        for algo in self.hedge_algos.values():
            algo_data = data.get(algo.chain_symbol)
            if algo_data:
                algo.balance_price = algo_data['balance_price']
                algo.up_price = algo_data['up_price']
                algo.down_price = algo_data['down_price']
        self.data = data
        self.write_log(f"期权对冲引擎数据载入成功")

    def save_data(self) -> None:
        for algo in self.hedge_algos.values():
            d = {}
            d['balance_price'] = algo.balance_price
            d['up_price'] = algo.up_price
            d['down_price'] = algo.down_price
            self.data[algo.chain_symbol] = d
        save_json(self.data_filename, self.data)
        self.write_log(f"期权对冲引擎数据保存成功")


    def init_counter(self) -> None:
        self.counters['check_delta'] = 0
        self.counters['calculate_balance'] = 0

    def init_chains(self) -> None:
        for portfolio in self.option_engine.active_portfolios.values():
            self.chains.update(portfolio.chains)

    def init_hedge_algos(self) -> None:
        for chain_symbol, chain in self.chains.items():
            algo = ChannelHedgeAlgo(chain_symbol, chain, self)
            self.hedge_algos[chain_symbol] = algo

    def init_engine(self) -> None:
        self.load_setting()
        self.load_data()

        if self.option_engine.inited:
            self.init_counter()
            self.init_chains()
            self.init_hedge_algos()
            self.register_event()            
            # print(self.hedge_algos)
            self.write_log(f"期权对冲引擎初始化完成")
        else:
            self.write_log(f"期权扩展主引擎尚未完成初始化")

    def start_all_auto_hedge(self) -> None:
        for algo in self.hedge_algos:
            algo.start_auto_hedge()

    def stop_all_auto_hedge(self) -> None:
        for algo in self.hedge_algos:
            algo.stop_auto_hedge()

    def start_hedge_algo(self, chain_symbol: str, parameters: Dict) -> None:
        algo = self.hedge_algos.get(chain_symbol)
        if algo:
            algo.start_auto_hedge(parameters)

    def stop_hedge_algo(self, chain_symbol: str) -> None:
        algo = self.hedge_algos.get(chain_symbol)
        if algo:
            algo.stop_auto_hedge()

    def register_event(self) -> None:
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_OPTION_STRATEGY_ORDER, self.process_strategy_order)

    def process_strategy_order(self, event: Event) -> None:
        strategy_order = event.data
        algo = self.hedge_algos[strategy_order.chain_symbol]

        if strategy_order.is_active():
            algo.active_strategyids.add(strategy_order.strategy_id)

        if strategy_order.is_finished():
            algo.active_strategyids.remove(strategy_order.strategy_id)
            if not algo.is_hedging():
                algo.status = HedgeStatus.RUNNING

            algo.calculate_balance_price()

    def process_timer_event(self, event: Event) -> None:
        # print('hedge timer event')
        # check_delta_counter = self.counters.get('check_delta')
        # calc_balance_counter = self.counters.get('calculate_balance')

        try:
            if self.counters['check_delta'] > self.check_delta_trigger:
                self.auto_hedge()
                self.counters['check_delta'] = 0

            if self.counters['calculate_balance'] > self.calc_balance_trigger:
                self.calc_all_balance()
                self.counters['calculate_balance'] = 0

            self.counters['check_delta'] += 1
            self.counters['calculate_balance'] += 1
        except:
            msg = f"处理委托事件，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def auto_hedge(self) -> None:
        for algo in self.hedge_algos.values():
            algo.check_hedge_signal()

    def calc_all_balance(self) -> None:
        for algo in self.hedge_algos.values():
            if algo.is_hedging():
                continue
            algo.calculate_balance_price()

    def put_hedge_algo_status_event(self, algo: "ChannelHedgeAlgo") -> None:
        event = Event(EVENT_OPTION_HEDGE_ALGO_STATUS, algo)
        self.event_engine.put(event)

    def write_log(self, msg: str):
        log = LogData(APP_NAME, msg)
        event = Event(EVENT_OPTION_HEDGE_ALGO_LOG, log)
        self.event_engine.put(event)

class ChannelHedgeAlgo:

    def __init__(self, chain_symbol: str, chain: ChainData, hedge_engine: HedgeEngine):
        self.chain_symbol = chain_symbol
        self.chain = chain
        self.hedge_engine = hedge_engine

        self.option_engine: OptionEngineExt = self.hedge_engine.option_engine
        self.underlying: UnderlyingData = self.chain.underlying

        # parameters
        self.offset_percent: float = 0.0
        self.hedge_percent: float = 0.0
        self.send_at_break = True

        # variables
        self.strategy_orders: Dict[str, OptionStrategyOrder] = {}
        self.active_strategyids: Set[str] = set()

        # self.active: bool = False
        self.status: HedgeStatus = HedgeStatus.NOTSTART

        self.balance_price: float = 0.0
        self.up_price: float = 0.0
        self.down_price: float = 0.0

        # self.net_pos: int = self.chain.net_pos
        # self.pos_delta: float = self.chain.pos_delta

        self.long_hedge_count: int = 0
        self.short_hedge_count: int = 0
        self.hedge_ref: int = 0

        self.write_log = self.hedge_engine.write_log
        self.parameters = ['offset_percent', 'hedge_percent']

    def is_hedging(self) -> bool:
        return len(self.active_strategyids) > 0

    def is_active(self) -> bool:
        return self.status == HedgeStatus.RUNNING or self.status == HedgeStatus.HEDGING

    def get_synthesis_atm(self) -> Tuple[OptionData, OptionData]:
        chain = self.chain
        print('get synthesis atm', chain.atm_index)
        atm_call = chain.calls[chain.atm_index]
        atm_put = chain.puts[chain.atm_index]
        return atm_call, atm_put

    def calculate_hedge_volume(self) -> int:
        atm_call, atm_put = self.get_synthesis_atm()
        unit_hedge_delta = abs(atm_call.cash_delta) + abs(atm_put.cash_delta)
        print('calculate hedge volume', unit_hedge_delta)
        if not unit_hedge_delta:
            return
        to_hedge_volume = abs(self.chain.pos_delta) * self.hedge_percent / unit_hedge_delta
        return round(to_hedge_volume)

    def calculate_pos_delta(self, price: float) -> float:
        """
        Calculate pos delta at specific price.
        """
        chain_delta = 0
        for option in self.chain.options.values():
            if option.net_pos:
                _price, delta, _gamma, _theta, _vega = option.calculate_greeks(
                    price,
                    option.strike_price,
                    option.interest_rate,
                    option.time_to_expiry,
                    option.mid_impv,
                    option.option_type
                )
                delta = delta * option.size * option.net_pos
                chain_delta += delta
        return chain_delta

    def calculate_balance_price(self) -> float:
        """
        Search balcance price by bisection method.
        """
        if not self.chain.net_pos:
            return

        left_end = 0
        right_end = 0
        pricetick = self.underlying.pricetick
        try_price = self.underlying.mid_price
        print('underlying price tick', try_price)

        while True:
            try_delta = self.calculate_pos_delta(try_price)
            if try_delta > 0:
                left_end = try_price
                # if right boudary is uncentain
                if right_end == 0 or try_price == right_end:
                    right_end = try_price * 1.05
                    try_price = right_end
                else:
                    try_price = (left_end + right_end) / 2
            elif try_delta < 0:
                right_end = try_price
                # if left boundary is uncertain
                if left_end == 0 or try_price == left_end:
                    left_end = try_price * 0.95
                    try_price = left_end
                else:
                    try_price = (left_end + right_end) / 2
            else:
                self.balance_price = try_price
                break

            if right_end - left_end < pricetick * 2:
                self.balance_price = (left_end + right_end) / 2
                break

        if self.offset_percent:
            self.up_price = self.balance_price * (1 + self.offset_percent)
            self.down_price = self.balance_price * (1 - self.offset_percent)

        self.put_hedge_algo_status_event(self)

    def start_auto_hedge(self, params: Dict) -> None:
        if self.is_active():
            return

        for param_name in self.parameters:
            if param_name in params:
                value = params[param_name]
                setattr(self, param_name, value)

        self.status = HedgeStatus.RUNNING
        self.put_hedge_algo_status_event(self)
        self.write_log(f"期权链{self.chain_symbol}自动对冲已启动")

    def stop_auto_hedge(self) -> None:
        if not self.is_active():
            return

        self.status = HedgeStatus.NOTSTART
        self.put_hedge_algo_status_event(self)
        self.write_log(f"期权链{self.chain_symbol}自动对冲已停止")

    def action_hedge(self, direction: Direction) -> None:
        atm_call, atm_put = self.get_synthesis_atm()
        to_hedge_volume = self.calculate_hedge_volume()
        if not to_hedge_volume:
            self.write_log(f"期权链{self.chain_symbol} Delta偏移量少于最小对冲单元值")
            return

        if direction == Direction.LONG:
            call_req = self.option_engine.buy(atm_call.vt_symbol, to_hedge_volume)
            put_req = self.option_engine.short(atm_put.vt_symbol, to_hedge_volume)
        elif direction == Direction.SHORT:
            call_req = self.option_engine.short(atm_call.vt_symbol, to_hedge_volume)
            put_req = self.option_engine.buy(atm_put.vt_symbol, to_hedge_volume)
        else:
            self.write_log(f"对冲只支持多或者空")

        self.hedge_ref += 1
        strategy_order = OptionStrategyOrder(
            chain_symbol=self.chain_symbol,
            strategy_name=OptionStrategy.SYNTHESIS,
            direction=direction,
            strategy_ref=self.hedge_ref,
            send_at_break=True
        )
        strategy_order.add_req(call_req)
        strategy_order.add_req(put_req)
        self.option_engine.send_strategy_order(strategy_order)

        self.status = HedgeStatus.HEDGING
        self.strategy_orders[strategy_order.strategy_id] = strategy_order
        self.active_strategyids.add(strategy_order.strategy_id)
        self.put_hedge_algo_status_event(self)

    def is_hedge_inited(self) -> bool:
        pass

    def check_hedge_signal(self) -> None:
        if not self.is_active():
            return

        if self.is_hedging():
            return

        if not self.up_price or not self.down_price:
            print('up and down is not ready')
            return

        if not self.chain.atm_index:
            print('atm index is not ready')
            return
        
        tick = self.underlying.tick
        print('check_hedge_signal', tick.last_price, self.up_price)
        if tick.last_price > self.up_price:
            self.action_hedge(Direction.LONG)
        elif tick.last_price < self.down_price:
            self.action_hedge(Direction.SHORT)
        else:
            return

    def put_hedge_algo_status_event(self, algo: "ChannelHedgeAlgo") -> None:
        self.hedge_engine.put_hedge_algo_status_event(algo)


class StrategyTrader:
    def __init__(self, option_engine: OptionEngineExt):
        self.option_engine: OptionEngineExt = option_engine
        self.main_engine: MainEngine = option_engine.main_engine
        self.event_engine: EventEngine = option_engine.event_engine

        self.pay_up: int = 0
        self.cancel_interval: int = 3
        self.max_volume: int = 30

        self.orderid_to_strategyid: Dict[str, str] = {}
        self.active_orderids: Set[str] = set()
        self.strategy_orders: Dict[str, OptionStrategyOrder] = {}
        self.cancel_counts: Dict[str, int] = {}

        self.register_event()

    def register_event(self) -> None:
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_order_event(self, event: Event) -> None:
        order: OrderData = event.data
        vt_orderid = order.vt_orderid

        if vt_orderid not in self.active_orderids:
            return

        if not order.is_active():
            strategy_id = self.orderid_to_strategyid[vt_orderid]
            strategy_order = self.strategy_orders[strategy_id]
            strategy_order.active_orderids.remove(vt_orderid)

            self.active_orderids.remove(vt_orderid)
            self.cancel_counts.pop(vt_orderid, None)

        if order.status == Status.CANCELLED:
            self.resend_order(order)

    def process_timer_event(self, event: Event) -> None:
        self.chase_order()
        self.send_order()

    def resend_order(self, order: OrderData) -> None:
        new_volume = order.volume - order.traded
        if new_volume:
            strategy_id = self.orderid_to_strategyid[order.vt_orderid]
            strategy_order = self.strategy_orders[strategy_id]

            new_req = self.generate_order_req(
                vt_symbol=order.vt_symbol,
                direction=order.direction,
                offset=order.offset,
                volume=new_volume,
            )
            strategy_order.add_req(new_req)

    def cancel_order(self, vt_orderid: str) -> None:
        order = self.main_engine.get_order(vt_orderid)
        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def chase_order(self) -> None:
        for strategy_order in self.strategy_orders.values():
            if not strategy_order.is_active():
                continue

            active_orders = strategy_order.active_orderids()
            if not active_orders:
                strategy_order.status = StrategyOrderStatus.FINISHED
                self.put_stategy_order_event(strategy_order)
            else:
                for vt_orderid in active_orders:
                    if self.cancel_counts[vt_orderid] > self.cancel_interval:
                        order = self.main_engine.get_order(vt_orderid)
                        if not self.is_contract_break(order.vt_symbol):
                            self.cancel_counts[vt_orderid] = 0
                            self.cancel_order(vt_orderid)
                        else:
                            self.cancel_counts[vt_orderid] = 0
                    self.cancel_counts[vt_orderid] += 1

    def send_order(self) -> None:
        for strategy_id, strategy_order in self.strategy_orders.items():
            if strategy_order.is_finished():
                continue

            if self.is_strategy_order_break(strategy_order) and not strategy_order.send_at_break:
                continue

            reqs = strategy_order.reqs
            while reqs:
                req = reqs.pop()

                contract = self.main_engine.get_contract(req.vt_symbol)
                if not req.price:
                    req.price = self.get_default_order_price(req.vt_symbol, req.direction)

                split_req_list = self.split_req(req)
                for split_req in split_req_list:
                    vt_orderid = self.main_engine.send_order(split_req, contract.gateway_name)
                    strategy_order.active_orderids.add(vt_orderid)

                    self.active_orderids.add(vt_orderid)
                    self.orderid_to_strategyid[vt_orderid] = strategy_id
                    self.cancel_counts[vt_orderid] = 0

            if not strategy_order.is_active():
                strategy_order.status == StrategyOrderStatus.SENDED
                self.put_stategy_order_event(strategy_order)

    def get_default_order_price(self, vt_symbol: str, direction: Direction) -> float:
        contract = self.main_engine.get_contract(vt_symbol)
        tick = self.main_engine.get_tick(vt_symbol)
        if direction == Direction.LONG:
            price = min(tick.ask_price_1 + contract.pricetick * self.pay_up, tick.limit_up)

        else:
            price = max(tick.bid_price_1 - contract.pricetick * self.pay_up, tick.limit_down)
        return price

    def is_strategy_order_break(self, strategy_order: OptionStrategyOrder) -> bool:
        for req in strategy_order.reqs:
            tick = self.main_engine.get_tick(req.vt_symbol)
            if not tick.ask_price_2 and not tick.bid_price_2:
                return True
        return False

    def is_contract_break(self, vt_symbol: str) -> bool:
        tick = self.main_engine.get_tick(vt_symbol)
        return not tick.ask_price_2 and not tick.bid_price_2

    def split_req(self, req: OrderRequest):
        if req.volume <= self.max_volume:
            return [req]

        max_count, remainder = divmod(req.volume, self.max_volume)

        req_max = copy(req)
        req_max.volume = self.max_volume
        req_list = [req_max for i in range(int(max_count))]

        if remainder:
            req_r = copy(req)
            req_r.volume = remainder
            req_list.append(req_r)
        return req_list

    def send_strategy_order(
        self,
        strategy_order: OptionStrategyOrder
    ) -> None:
        self.strategy_orders[strategy_order.strategy_id] = strategy_order

    def generate_order_req(
        self,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        volume: float,
        price: float = 0
    ) -> OrderRequest:
        contract = self.main_engine.get_contract(vt_symbol)
        req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            type=OrderType.LIMIT,
            volume=volume,
            price=price 
        )
        return req

    def buy(self, vt_symbol: str, volume: float, price: float = 0):
        return self.generate_order_req(vt_symbol, Direction.LONG, Offset.OPEN, volume, price)

    def sell(self, vt_symbol: str, volume: float, price: float = 0):
        return self.generate_order_req(vt_symbol, Direction.SHORT, Offset.CLOSE, volume, price)

    def short(self, vt_symbol: str, volume: float, price: float = 0):
        return self.generate_order_req(vt_symbol, Direction.SHORT, Offset.OPEN, volume, price)

    def cover(self, vt_symbol: str, volume: float, price: float = 0):
        return self.generate_order_req(vt_symbol, Direction.LONG, Offset.CLOSE, volume, price)

    def put_stategy_order_event(self, strategy_order: OptionStrategyOrder) -> None:
        event = Event(EVENT_OPTION_STRATEGY_ORDER, strategy_order)
        self.event_engine.put(event)