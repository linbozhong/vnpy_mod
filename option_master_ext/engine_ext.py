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

EVENT_OPTION_HEDGE_STATUS = "eOptionHedgeStatus"
EVENT_OPTION_STRATEGY_TRADE_FINISHED = "eOptionStrategyTradeFinished"

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


@dataclass
class OptionStrategyOrder(BaseData):
    """
    Candlestick bar data of a certain trading period.
    """

    chain_symbol: str
    strategy_name: OptionStrategy = None
    direction: Direction = None



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

        self.add_order_function()

    def add_order_function(self) -> None:
        self.buy = self.strategy_trader.buy
        self.sell = self.strategy_trader.sell
        self.short = self.strategy_trader.short
        self.cover = self.strategy_trader.cover


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
        self.event_engine.register(EVENT_OPTION_STRATEGY_TRADE_FINISHED, self.process_trade_finished)

    def process_trade_finished(self, event: Event) -> None:
        strategy_id = event.data
        strategy_name, chain_symbol, count = strategy_id.split('_')

        if strategy_name == STRATEGY_HEDGE_DELTA_LONG or strategy_name == STRATEGY_HEDGE_DELTA_SHORT:
            algo = self.hedge_algos[chain_symbol]
            algo.calculate_balance_price()
            self.write_log(f"策略号{strategy_id}执行完毕.") 

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

        self.chain: ChainData = self.portfolio.get_chain(self.chain_symbol)
        self.underlying: UnderlyingData = self.chain.underlying

        # parameters
        self.offset_percent: float = 0.0
        self.hedge_percent: float = 0.0
        self.ignore_circuit_breaker = True

        # variables
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

        if not self.ignore_circuit_breaker:
            call_tick = self.hedge_engine.main_engine.get_tick(atm_call.vt_symbol)
            put_tick = self.hedge_engine.main_engine.get_tick(atm_put.vt_symbol)

            call_circuit_breaker = not call_tick.bid_price_2 and not call_tick.ask_price_2
            put_circuit_breaker = not put_tick.bid_price_2 and not put_tick.ask_price_2

            if call_circuit_breaker or put_circuit_breaker:
                self.write_log(f"期权链{self.chain_symbol}合成期权合约触发熔断机制，请稍后重试")
                return

        self.hedge_ref += 1
        if direction == Direction.LONG:
            strategy_id = f"{STRATEGY_HEDGE_DELTA_LONG}_{self.chain_symbol}_{self.hedge_ref}"
            self.hedge_engine.option_engine.buy(strategy_id, atm_call.vt_symbol, to_hedge_volume)
            self.hedge_engine.option_engine.sell(strategy_id, atm_put.vt_symbol, to_hedge_volume)
        elif direction == Direction.SHORT:
            strategy_id = f"{STRATEGY_HEDGE_DELTA_SHORT}_{self.chain_symbol}_{self.hedge_ref}"
            self.hedge_engine.option_engine.buy(strategy_id, atm_put.vt_symbol, to_hedge_volume)
            self.hedge_engine.option_engine.sell(strategy_id, atm_call.vt_symbol, to_hedge_volume)
        else:
            self.write_log(f"对冲只支持多或者空")

    def check_hedge_signal(self) -> None:
        if not self.active:
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

        self.strategy_active_orderids: Dict[str, Set[str]] = {}
        self.orderid_to_strategyid: Dict[str, str] = {}
        self.active_orderids: Set[str] = set()
        self.strategy_finished: Dict[str, bool] = {}
        
        self.pay_up: int = 0
        self.cancel_interval: int = 3
        self.max_volume: int = 30
        
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
            self.strategy_active_orderids[strategy_id].remove(vt_orderid)

            self.active_orderids.remove(vt_orderid)
            self.cancel_counts.pop(vt_orderid, None)

        if order.status == Status.CANCELLED:
            self.resend_order(order)

    def process_timer_event(self, event: Event) -> None:
        self.check_cancel()

    def resend_order(self, order: OrderData) -> None:
        new_volume = order.volume - order.traded
        if new_volume:
            strategy_id = self.orderid_to_strategyid[order.vt_orderid]
            self.send_order(strategy_id, order.vt_symbol, order.direction, order.offset, new_volume)

    def cancel_order(self, vt_orderid: str) -> None:
        order = self.main_engine.get_order(vt_orderid)
        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def check_cancel(self) -> None:
        for strategy_id, orders in self.strategy_active_orderids.items():
            if self.strategy_finished[strategy_id]:
                continue

            if not orders:
                self.strategy_finished[strategy_id] = True
                self.put_stategy_trade_finished_event(strategy_id)
                continue

            for vt_orderid in orders:
                if self.cancel_counts[vt_orderid] > self.cancel_interval:
                    order = self.main_engine.get_order(vt_orderid)
                    tick = self.main_engine.get_tick(order.vt_symbol)
                    if tick.bid_price_2 and tick.ask_price_2:
                        self.cancel_counts[vt_orderid] = 0
                        self.cancel_order(vt_orderid)
                self.cancel_counts[vt_orderid] += 1

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

    def send_order(
        self,
        strategy_id: str,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        volume: float,
        price: Optional[float] = None
    ) -> str:
        contract = self.main_engine.get_contract(vt_symbol)
        tick = self.main_engine.get_tick(vt_symbol)

        if not price:
            if direction == Direction.LONG:
                price = tick.ask_price_1 + contract.pricetick * self.pay_up
            else:
                price = tick.bid_price_1 - contract.pricetick * self.pay_up

        original_req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            type=OrderType.LIMIT,
            volume=volume,
            price=price
        )

        splited_req_list = self.split_req(original_req)

        strategy_orders = self.strategy_active_orderids.get(strategy_id)
        if strategy_orders is None:
            strategy_orders = set()
            self.strategy_active_orderids[strategy_id] = strategy_orders

        vt_orderids = []
        for req in splited_req_list:
            vt_orderid = self.main_engine.send_order(req, contract.gateway_name)
            strategy_orders.add(vt_orderid)
            self.active_orderids.add(vt_orderid)
            self.orderid_to_strategyid[vt_orderid] = strategy_id
            vt_orderids.append(vt_orderid)
            self.cancel_counts[vt_orderid] = 0

        return vt_orderids

    def buy(self, strategy_id: str, vt_symbol: str, volume: float, price: Optional[float] = None):
        return self.send_order(strategy_id, vt_symbol, Direction.LONG, Offset.OPEN, volume, price)

    def sell(self, strategy_id: str, vt_symbol: str, volume: float, price: Optional[float] = None):
        return self.send_order(strategy_id, vt_symbol, Direction.SHORT, Offset.CLOSE, volume, price)

    def short(self, strategy_id: str, vt_symbol: str, volume: float, price: Optional[float] = None):
        return self.send_order(strategy_id, vt_symbol, Direction.SHORT, Offset.OPEN, volume, price)

    def cover(self, strategy_id: str, vt_symbol: str, volume: float, price: Optional[float] = None):
        return self.send_order(strategy_id, vt_symbol, Direction.LONG, Offset.CLOSE, volume, price)

    def put_stategy_trade_finished_event(self, strategy_id: str) -> None:
        event = Event(EVENT_OPTION_STRATEGY_TRADE_FINISHED, strategy_id)
        self.event_engine.put(event)