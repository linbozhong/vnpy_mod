import typing
from typing import Optional, Dict, List, Set, Callable, Tuple
from copy import copy
from enum import Enum
from dataclasses import dataclass

from vnpy.event import Event, EventEngine
from vnpy.trader.event import (
    EVENT_TIMER, EVENT_ORDER
)
from vnpy.trader.constant import (
    Status, Direction, Offset
)
from vnpy.trader.object import (
    BaseData,
    OrderData, OrderRequest, OrderType
)
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.app.option_master.engine import OptionEngine
from vnpy.app.option_master.base import (
    CHAIN_UNDERLYING_MAP,
    OptionData, PortfolioData, UnderlyingData, ChainData
)

APP_NAME = "OptionMasterExt"

EVENT_OPTION_STRATEGY_ORDER = "eOptionStrategyOrder"
EVENT_OPTION_HEDGE_STATUS = "eOptionHedgeStatus"

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
    LISTENING = "监测"
    HEDGING = "对冲中"

class StrategyOrderStatus(Enum):
    SUBMITTING = "提交中"
    SENDED = "已发送"
    FINISHED = "已完成"


@dataclass
class OptionStrategyOrder:
    chain_symbol: str
    strategy_name: OptionStrategy
    direction: Direction
    strategy_ref: int
    send_at_break: bool
    status: StrategyOrderStatus = StrategyOrderStatus.SUBMITTING
    reqs: List[OrderRequest] = []
    active_orderids: Set[str] = set()

    def __post_init__(self):
        """"""
        self.strategy_id = f"{self.strategy_name.value}.{self.direction.value}.{self.strategy_ref}"

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

        self.hedge_engine: "HedgeEngine" = HedgeEngine(self)
        self.strategy_trader: "StrategyTrader" = StrategyTrader(self)

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


class HedgeEngine:
    def __init__(self, option_engine: OptionEngineExt):
        self.option_engine: OptionEngineExt = option_engine
        self.main_engine: MainEngine = option_engine.main_engine
        self.event_engine: EventEngine = option_engine.event_engine
        self.strategy_trader: "StrategyTrader" = option_engine.strategy_trader

        # parameters
        self.check_delta_trigger: int = 5
        self.calc_balance_trigger: int = 300
        self.chanel_width: float = 0.0
        self.hedge_percent: float = 0.0

        # variables
        self.hedge_algos: Dict[str, "ChannelHedgeAlgo"] = {}

        # self.balance_prices: Dict[str, float] = {}
        # self.underlyings: Dict[str, UnderlyingData] = {}
        # self.underlying_symbols: Dict[str, str] = {}
        # self.synthesis_chain_symbols: Dict[str, str] = {}
        # self.auto_portfolio_names: List[str] = []
        self.counters: Dict[str, float] = {}
        # self.auto_hedge_flags: Dict[str, bool] = {}
        # self.hedge_parameters: Dict[str, Dict] = {}

        # init
        self.init_hedge_algos()
        self.init_counter()


    def start_all_auto_hedge(self):
        for algo in self.hedge_algos:
            algo.start_auto_hedge()

    def stop_all_auto_hedge(self, portfolio_name: str):
        for algo in self.hedge_algos:
            algo.stop_auto_hedge()

    def init_counter(self) -> None:
        self.counters['check_delta'] = 0
        self.counters['calculate_balance'] = 0

    def init_hedge_algos(self) -> None:
        for portfolio in self.option_engine.active_portfolios.values():
            for chain_symbol in portfolio.chains:
                algo = ChannelHedgeAlgo(chain_symbol, self, portfolio)
                self.hedge_algos[chain_symbol] = algo

    def register_event(self) -> None:
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_OPTION_STRATEGY_ORDER, self.process_strategy_order)

    def process_strategy_order(self, event: Event) -> None:
        strategy_order = event.data
        pass

    def process_timer_event(self, event: Event) -> None:
        check_delta_counter = self.counters.get('check_delta')
        calc_balance_counter = self.counters.get('calculate_balance')

        if check_delta_counter > self.check_delta_trigger:
            self.auto_hedge()
            check_delta_counter = 0

        if calc_balance_counter > self.calc_balance_trigger:
            self.calc_all_balance()
            calc_balance_counter = 0

        check_delta_counter += 1
        calc_balance_counter += 1

    def put_hedge_status_event(self) -> None:
        status = copy(self.auto_hedge_flags)
        event = Event(EVENT_OPTION_HEDGE_STATUS, status)
        self.event_engine.put(event)

    def write_log(self, msg: str):
        self.main_engine.write_log(msg, source=APP_NAME)


class ChannelHedgeAlgo:

    def __init__(self, chain_symbol: str, hedge_engine: HedgeEngine, portfolio: PortfolioData):
        self.chain_symbol = chain_symbol
        self.hedge_engine = hedge_engine
        self.portfolio = portfolio

        self.option_engine: OptionEngineExt = self.hedge_engine.option_engine
        self.chain: ChainData = self.portfolio.get_chain(self.chain_symbol)
        self.underlying: UnderlyingData = self.chain.underlying

        # parameters
        self.offset_percent: float = 0.0
        self.hedge_percent: float = 0.0
        self.send_at_break = True

        # variables
        self.strategy_orders: Dict[str, OptionStrategyOrder] = {}
        self.active_strategyids: Set[str] = set()

        self.active: bool = False

        self.balance_price: float = 0.0
        self.up_price: float = 0.0
        self.down_price: float = 0.0

        self.net_pos: int = self.chain.net_pos
        self.pos_delta: float = self.chain.pos_delta

        self.long_hedge_count: int = 0
        self.short_hedge_count: int = 0
        self.hedge_ref: int = 0

        self.write_log = self.hedge_engine.write_log
        self.parameters = ['offset_percent', 'hedge_percent']

    def get_synthesis_atm(self) -> Tuple[OptionData, OptionData]:
        chain = self.chain
        atm_call = chain.calls[chain.atm_index]
        atm_put = chain.puts[chain.atm_index]
        return atm_call, atm_put

    def calculate_hedge_volume(self) -> int:
        atm_call, atm_put = self.get_synthesis_atm()
        unit_hedge_delta = abs(atm_call.cash_delta) + abs(atm_put.cash_delta)
        to_hedge_volume = abs(self.pos_delta) * self.hedge_percent / unit_hedge_delta
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
        left_end = 0
        right_end = 0
        pricetick = self.underlying.pricetick
        try_price = self.underlying.mid_price
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

        self.up_price = self.balance_price * (1 + self.offset_percent)
        self.down_price = self.balance_price * (1 - self.offset_percent)

    def start_auto_hedge(self, params: Dict):
        if self.active:
            return

        for param_name in self.parameters:
            if param_name in params:
                value = params[param_name]
                setattr(self, param_name, value)

        self.active = True
        self.put_hedge_status_event()
        self.write_log(f"期权链{self.chain_symbol}自动对冲已启动")

    def stop_auto_hedge(self, portfolio_name: str):
        if not self.active:
            return

        self.active = False
        self.put_hedge_status_event()
        self.write_log(f"期权链{self.chain_symbol}自动对冲已停止")

    def action_hedge(self, direction: Direction):
        atm_call, atm_put = self.get_synthesis_atm()
        to_hedge_volume = self.calculate_hedge_volume()
        if not to_hedge_volume:
            self.write_log(f"期权链{self.chain_symbol} Delta偏移量少于最小对冲单元值")
            return

        self.hedge_ref += 1
        if direction == Direction.LONG:
            call_req = self.option_engine.buy(atm_call.vt_symbol, to_hedge_volume)
            put_req = self.option_engine.short(atm_put.vt_symbol, to_hedge_volume)
        elif direction == Direction.SHORT:
            call_req = self.option_engine.short(atm_call.vt_symbol, to_hedge_volume)
            put_req = self.option_engine.buy(atm_put.vt_symbol, to_hedge_volume)
        else:
            self.write_log(f"对冲只支持多或者空")

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

        self.strategy_orders[strategy_order.strategy_id] = strategy_order
        self.active_strategyids.add(strategy_order.strategy_id)

    def check_hedge_signal(self) -> None:
        if not self.active:
            return

        if self.active_strategyids:
            return
        
        tick = self.underlying.tick
        if tick.last_price > self.up_price:
            self.action_hedge(Direction.LONG)
        elif tick.last_price < self.down_price:
            self.action_hedge(Direction.SHORT)
        else:
            return

    def put_hedge_status_event(self) -> None:
        # status = copy(self.auto_hedge_flags)
        # event = Event(EVENT_OPTION_HEDGE_STATUS, status)
        # self.event_engine.put(event)
        pass


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