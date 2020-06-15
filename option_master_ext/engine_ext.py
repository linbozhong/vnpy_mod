import traceback
import typing
from typing import Optional, Dict, List, Set, Callable, Tuple, Any
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
    CALL = "认购"
    PUT = "认沽"
    SYNTHESIS = "合成"
    STRADDLE = "跨式"
    STRANGLE = "宽跨式"
    CALL_BULL_SPREAD = "认购牛市价差"
    PUT_BULL_SPREAD = "认沽牛市价差"
    PUT_BEAR_SPREAD = "认沽熊市价差"
    CALL_BEAR_SPREAD = "认购熊市价差"


class HedgeStatus(Enum):
    NOTSTART = "未启动"
    RUNNING = "监测中"
    HEDGING = "对冲中"

class StrategyOrderStatus(Enum):
    SUBMITTING = "提交中"
    SENDED = "已发送"
    FINISHED = "已完成"
    CANCELLED = "已撤销"


class OptionStrategyOrder:

    def __init__(
        self,
        chain_symbol: str,
        strategy_name: OptionStrategy,
        direction: Direction,
        send_at_break: bool,
        strategy_ref: int = 1,
    ):
        self.chain_symbol = chain_symbol
        self.strategy_name = strategy_name
        self.direction = direction
        self.strategy_ref = strategy_ref
        self.send_at_break = send_at_break

        self.strategy_id: str = f"{self.chain_symbol}.{self.strategy_name.value}.{self.direction.value}.{self.strategy_ref}"
        self.time: str = datetime.now().strftime("%H:%M:%S")
        self.legs_symbol: str = ""

        self.status: StrategyOrderStatus = StrategyOrderStatus.SUBMITTING

        self.leg_names: List[str] = [] 
        self.legs: List[dict] = []
        self.reqs: List[OrderRequest] = []
        self.active_orderids: Set[str] = set()

        self.cancel_count: int = 0

    def is_finished(self) -> bool:
        return self.status == StrategyOrderStatus.FINISHED

    def is_active(self) -> bool:
        return self.status == StrategyOrderStatus.SENDED

    def convert_leg_to_dict(self, option: OptionData, volume: int, price: float = 0) -> dict:
        d = {}
        d['vt_symbol'] = option.vt_symbol
        d['volume'] = volume
        d['price'] = price
        return d

    def long_call(self, call: OptionData, volume: int, price: float = 0):
        d = self.convert_leg_to_dict(call, volume, price)
        d['direction'] = Direction.LONG
        leg_name = f"LC{call.strike_price}@{volume}"
        self.leg_names.append(leg_name)
        self.legs.append(d)

    def long_put(self, put: OptionData, volume: int, price: float = 0):
        d = self.convert_leg_to_dict(put, volume, price)
        d['direction'] = Direction.LONG
        leg_name = f"LP{put.strike_price}@{volume}"
        self.leg_names.append(leg_name)
        self.legs.append(d)

    def short_call(self, call: OptionData, volume: int, price: float = 0):
        d = self.convert_leg_to_dict(call, volume, price)
        d['direction'] = Direction.SHORT
        leg_name = f"SC{call.strike_price}@{volume}"
        self.leg_names.append(leg_name)
        self.legs.append(d)

    def short_put(self, put: OptionData, volume: int, price: float = 0):
        d = self.convert_leg_to_dict(put, volume, price)
        d['direction'] = Direction.SHORT
        leg_name = f"SP{put.strike_price}@{volume}"
        self.leg_names.append(leg_name)
        self.legs.append(d)

    def add_req(self, req: OrderRequest):
        self.reqs.append(req)

    def get_legs_symbol(self):
        self.legs_symbol = '_'.join(self.leg_names)


class OptionEngineExt(OptionEngine):
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__(main_engine, event_engine)
        self.engine_name = APP_NAME

        self.inited: bool = False

        self.strategy_trader: "StrategyTrader" = StrategyTrader(self)
        self.margin_calculator: "MarginCaculator" = MarginCaculator(self)

        self.hedge_engine: "HedgeEngine" = HedgeEngine(self)

        # containers
        self.chains: Dict[str, ChainData] = {}

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
        self.init_chains()
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

    def init_chains(self) -> None:
        for portfolio in self.active_portfolios.values():
            self.chains.update(portfolio.chains)

    def process_trade_event(self, event: Event) -> None:
        super().process_trade_event(event)

        trade = event.data
        # print('ext trade event')
        if not self.hedge_engine.inited:
            # print('hedge engine is un ready')
            return

        instrument = self.instruments.get(trade.vt_symbol, None)
        if not instrument:
            return

        if isinstance(instrument, OptionData):
            # print('is option data:', instrument)
            chain_symbol = instrument.chain.chain_symbol
            algo = self.hedge_engine.hedge_algos.get(chain_symbol)
            if algo:
                algo.calculate_balance_price()


class StrategyTrading():

    def __init__(self, option_engine: OptionEngineExt):
        self.option_engine = option_engine
        self.margin_calc: "MarginCaculator" = option_engine.margin_calculator

        self.etf_m_indices: Dict[str, list] = {}

        self.capital: float = 0.0

    def set_capital(self, capital: float):
        self.capital = capital

    def calculate_volume(self, money: float, call: OptionData, put: OptionData):
        ratio = call.cash_delta / put.cash_delta
        call_margin = self.margin_calc.get_margin(call.vt_symbol)
        put_margin = self.margin_calc.get_margin(put.vt_symbol)
        call_volume = round(money / (call_margin + put_margin * ratio))
        put_volume = round(call_volume * ratio)
        return call_volume, put_volume

    def get_etf_m_atm(self, chain_symbol: str, exclude_a: bool = True):
        chain = self.option_engine.chains.get(chain_symbol)
        if not chain.atm_index:
            pass
            return

        if not exclude_a:
            return chain.atm_index
        else:
            underlying_price = chain.underlying.mid_price

            atm_distance = 0
            atm_index = ""

            for call in chain.calls.values():
                if 'A' in call.chain_index:
                    continue

                price_distance = abs(underlying_price - call.strike_price)

                if not atm_distance or price_distance < atm_distance:
                    atm_distance = price_distance
                    atm_index = call.chain_index

            return atm_index

    def get_etf_m_indices(self, chain_symbol: str) -> List[str]:
        indices = self.etf_m_indices.get(chain_symbol)
        if indices is None:
            chain = self.option_engine.chains.get(chain_symbol)
            indices = [index for index in chain.indexes if 'A' not in index]
            self.etf_m_indices[chain_symbol] = indices
        return indices

    def get_levels(self, chain_symbol: str) -> Tuple[List[int]]:
        indices = self.get_etf_m_indices(chain_symbol)
        atm_index = self.get_etf_m_atm(chain_symbol)
        
        atm_idx = indices.index(atm_index)
        indices_ids = list(range(len(indices)))

        call_levels = [i - atm_idx for i in indices_ids]
        put_levels = [-(i - atm_idx) for i in indices_ids]
        return call_levels, put_levels

    # def is_in_levels(self, chain_symbol: str, call_level: int, put_level: int) -> bool:
    #     call_levels, put_levels = self.get_levels(chain_symbol)
    #     return call_level in call_levels and put_level in put_levels

    def is_in_call_levels(self, chain_symbol: str, call_level: int) -> bool:
        call_levels, _put_levels = self.get_levels(chain_symbol)
        return call_level in call_levels

    def is_in_put_levels(self, chain_symbol: str, put_level: int) -> bool:
        _call_levels, put_levels = self.get_levels(chain_symbol)
        return put_level in put_levels

    def get_straddle_legs(self, chain_symbol: str) -> Tuple[OptionData]:
        chain, _indices, atm_index = self.get_legs_basic(chain_symbol)

        atm_call = chain.calls[atm_index]
        atm_put = chain.puts[atm_index]
        return atm_call, atm_put

    def get_legs_basic(self, chain_symbol: str) -> Tuple[Any]:
        chain = self.option_engine.chains.get(chain_symbol)
        indices = self.get_etf_m_indices(chain_symbol)
        atm_index = self.get_etf_m_atm(chain_symbol)
        return chain, indices, atm_index

    def get_strangle_legs(self, chain_symbol:str, call_level: int, put_level: int) -> Tuple[OptionData]:
        call = self.get_call(chain_symbol, call_level)
        put = self.get_put(chain_symbol, put_level)
        return call, put

    def get_bull_call_spread(self, chain_symbol:str, buy_level: int, short_level: int) -> Tuple[OptionData]:
        if buy_level >= short_level:
            pass
            return

        buy_call = self.get_call(chain_symbol, buy_level)
        short_call = self.get_call(chain_symbol, short_level)
        return buy_call, short_call

    def get_bear_put_spread(self, chain_symbol:str, buy_level: int, short_level: int) -> Tuple[OptionData]:
        if buy_level >= short_level:
            pass
            return

        buy_put = self.get_put(chain_symbol, buy_level)
        short_put = self.get_put(chain_symbol, short_level)
        return buy_put, short_put

    def get_bull_put_spread(self, chain_symbol:str, short_level: int, buy_level: int) -> Tuple[OptionData]:
        if buy_level <= short_level:
            pass
            return

        short_put = self.get_put(chain_symbol, short_level)
        buy_put = self.get_put(chain_symbol, buy_level)
        return short_put, buy_put

    def get_bear_call_spread(self, chain_symbol:str, short_level: int, buy_level: int) -> Tuple[OptionData]:
        if buy_level <= short_level:
            pass
            return

        short_call = self.get_call(chain_symbol, short_level)
        buy_call = self.get_call(chain_symbol, buy_level)
        return short_call, buy_call

    def get_call(self, chain_symbol: str, level: int):
        chain, indices, atm_index = self.get_legs_basic(chain_symbol)
        if not self.is_in_call_levels(chain_symbol, level):
            pass
            return

        atm_idx = indices.index(atm_index)
        call_idx = atm_idx + level
        call = chain.calls[indices[call_idx]]
        return call

    def get_put(self, chain_symbol: str, level: int):
        chain, indices, atm_index = self.get_legs_basic(chain_symbol)
        if not self.is_in_put_levels(chain_symbol, level):
            pass
            return

        atm_idx = indices.index(atm_index)
        put_idx = atm_idx + level
        put = chain.calls[indices[put_idx]]
        return put

    def get_last_price(self, option: OptionData) -> float:
        option = self.option_engine.instruments.get(option.vt_symbol)
        return option.tick.last_price

    def long_call(self, chain_symbol: str, level: int, risk_rate: float) -> OptionStrategyOrder:
        call = self.get_call(chain_symbol, level)
        last_price = self.get_last_price(call)
        volume = round(self.capital * risk_rate / last_price)

        strategy_name = OptionStrategy.CALL
        strategy = OptionStrategyOrder(chain_symbol, strategy_name, Direction.LONG, False)
        strategy.long_call(call, volume)
        return strategy

    def long_put(self, chain_symbol: str, level: int, risk_rate: float) -> OptionStrategyOrder:
        put = self.get_put(chain_symbol, level)
        last_price = self.get_last_price(put)
        volume = round(self.capital * risk_rate / last_price)

        strategy_name = OptionStrategy.PUT
        strategy = OptionStrategyOrder(chain_symbol, strategy_name, Direction.LONG, False)
        strategy.long_put(put, volume)
        return strategy

    def short_call(self, chain_symbol: str, level: int, risk_rate: float) -> OptionStrategyOrder:
        call = self.get_call(chain_symbol, level)
        margin = self.margin_calc.calculate_etf_margin(call)
        volume = round(self.capital * risk_rate / margin)

        strategy_name = OptionStrategy.CALL
        strategy = OptionStrategyOrder(chain_symbol, strategy_name, Direction.SHORT, False)
        strategy.short_call(call, volume)
        return strategy

    def short_put(self, chain_symbol: str, level: int, risk_rate: float) -> OptionStrategyOrder:
        put = self.get_put(chain_symbol, level)
        margin = self.margin_calc.calculate_etf_margin(put)
        volume = round(self.capital * risk_rate / margin)

        strategy_name = OptionStrategy.PUT
        strategy = OptionStrategyOrder(chain_symbol, strategy_name, Direction.SHORT, False)
        strategy.short_put(put, volume)
        return strategy

class HedgeEngine:

    setting_filename = "channel_hedge_algo_setting.json"
    data_filename = "channel_hedge_algo_data.json"

    def __init__(self, option_engine: OptionEngineExt):
        self.option_engine = option_engine

        self.main_engine: MainEngine = option_engine.main_engine
        self.event_engine: EventEngine = option_engine.event_engine
        self.strategy_trader: "StrategyTrader" = option_engine.strategy_trader

        # parameters
        self.check_delta_trigger: int = 5
        self.calc_balance_trigger: int = 300
        self.offset_percent: float = 0.0
        self.hedge_percent: float = 0.0

        # variables
        self.inited = False

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
                algo.offset_percent = algo_setting['offset_percent']
                algo.hedge_percent = algo_setting['hedge_percent']
                self.put_hedge_algo_status_event(algo)
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
                self.put_hedge_algo_status_event(algo)
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
        if self.option_engine.inited:
            self.init_counter()
            self.init_chains()
            self.init_hedge_algos()
            self.register_event()

            self.load_setting()
            self.load_data()

            self.inited = True     
            self.write_log(f"期权对冲引擎初始化完成")
        else:
            self.write_log(f"期权扩展主引擎尚未完成初始化")


    def stop_all_auto_hedge(self) -> None:
        for algo in self.hedge_algos.values():
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
            self.put_hedge_algo_status_event(algo)
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
                if not option.mid_impv:
                    print(f'{option.vt_symbol} mid-impv is not')
                    return

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

        while True:
            try_delta = self.calculate_pos_delta(try_price)
            if not try_delta:
                return

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

        if not self.chain.net_pos:
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
        if not self.is_active():
            # print('algo is stop')
            return False

        if self.is_hedging():
            print('algo is hedgeing')
            return False

        if not self.balance_price or not self.up_price or not self.down_price:
            print('up and down is not ready')
            return False

        if not self.chain.atm_index:
            print('atm index is not ready')
            return False

        return True

    def check_hedge_signal(self) -> None:
        if not self.is_hedge_inited():
            return
        
        tick = self.underlying.tick
        # print('check_hedge_signal', tick.last_price, self.up_price)
        if tick.last_price > self.up_price:
            self.action_hedge(Direction.LONG)
        elif tick.last_price < self.down_price:
            self.action_hedge(Direction.SHORT)
        else:
            return

    def manual_hedge(self) -> None:
        if not self.is_hedge_inited():
            return

        if self.chain.pos_delta > 0:
            self.action_hedge(Direction.SHORT)
        else:
            self.action_hedge(Direction.LONG)

    def put_hedge_algo_status_event(self, algo: "ChannelHedgeAlgo") -> None:
        self.hedge_engine.put_hedge_algo_status_event(algo)


class StrategyTrader:
    def __init__(self, option_engine: OptionEngineExt):
        self.option_engine: OptionEngineExt = option_engine
        self.main_engine: MainEngine = option_engine.main_engine
        self.event_engine: EventEngine = option_engine.event_engine

        self.pay_up: int = -5
        self.cancel_interval: int = 3
        self.max_volume: int = 30
        self.max_resend: int = 3

        self.orderid_to_strategyid: Dict[str, str] = {}
        self.active_orderids: Set[str] = set()
        self.strategy_orders: Dict[str, OptionStrategyOrder] = {}
        self.cancel_counts: Dict[str, int] = {}
        
        self.child_orders: Dict[str, Set[str]] = {}
        self.orderid_to_parentid: Dict[str, str] = {}

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
        strategy_id = self.orderid_to_strategyid[order.vt_orderid]
        strategy_order = self.strategy_orders[strategy_id]

        parent_id = self.orderid_to_parentid.get(order.vt_orderid)
        child_count = len(self.child_orders[parent_id])
        if child_count > self.max_resend:
            strategy_order.status == StrategyOrderStatus.CANCELLED
            self.put_stategy_order_event(strategy_order)
            return

        new_volume = order.volume - order.traded
        new_req = self.generate_order_req(
            vt_symbol=order.vt_symbol,
            direction=order.direction,
            offset=order.offset,
            volume=new_volume,
        )
        new_req.price = self.get_default_order_price(new_req.vt_symbol, new_req.direction)
        vt_orderid = self.main_engine.send_order(new_req, order.gateway_name)

        strategy_order.active_orderids.add(vt_orderid)
        self.active_orderids.add(vt_orderid)
        self.orderid_to_strategyid[vt_orderid] = strategy_id
        self.cancel_counts[vt_orderid] = 0
        
        self.child_orders[parent_id].add(vt_orderid)
        self.orderid_to_parentid[vt_orderid] = parent_id

    def cancel_order(self, vt_orderid: str) -> None:
        order = self.main_engine.get_order(vt_orderid)
        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def chase_order(self) -> None:
        for strategy_order in self.strategy_orders.values():
            if not strategy_order.is_active():
                continue

            active_orders = strategy_order.active_orderids
            if not active_orders and not strategy_order.reqs:
                strategy_order.status = StrategyOrderStatus.FINISHED
                self.put_stategy_order_event(strategy_order)
            else:
                for vt_orderid in active_orders:
                    if self.cancel_counts[vt_orderid] > self.cancel_interval:
                        print('cancel order:', vt_orderid, self.cancel_counts[vt_orderid])
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
                print('every req sending..')
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

                    child_set = set()
                    child_set.add(vt_orderid)
                    self.child_orders[vt_orderid] = child_set
                    self.orderid_to_parentid[vt_orderid] = vt_orderid

            if not strategy_order.is_active():
                strategy_order.status = StrategyOrderStatus.SENDED

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
        strategy_order.get_legs_symbol()
        for leg in strategy_order.legs:
            if leg['direction'] == Direction.LONG:
                req = self.buy(leg['vt_symbol'], leg['volume'], leg['price'])
            else:
                req = self.short(leg['vt_symbol'], leg['volume'], leg['price'])
            strategy_order.add_req(req)

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
            offset=offset,
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


class MarginCaculator:

    def __init__(self, option_engine: OptionEngineExt):
        self.option_engine = option_engine

        self.margins: Dict[str, float] = {}

    def get_margin(self, vt_symbol: str):
        margin = self.margins.get(vt_symbol)
        if not margin:
            option = self.option_engine.instruments.get(vt_symbol)
            margin = self.calculate_etf_margin(option)
            self.margins[vt_symbol] = margin
            return margin

    def calculate_etf_margin(self, option: OptionData):
        option_pre_close = option.tick.pre_close
        underlying_pre_close = option.underlying.tick.pre_close

        size = option.size
        opc = option_pre_close
        upc = underlying_pre_close

        if option.option_type == 1:
            otm = max(option.strike_price - underlying_pre_close, 0)
            margin = (opc + max(upc * 0.12 - otm, upc * 0.07)) * size
        else:
            otm = max(underlying_pre_close - option.strike_price, 0)
            margin = min(opc + max(upc * 0.12 - otm, upc * 0.07), option.strike_price) * size
        
        return margin
