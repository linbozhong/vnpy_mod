import typing
from typing import Optional, Dict, List, Set


from vnpy.event import Event, EventEngine
from vnpy.trader.event import (
    EVENT_TIMER, EVENT_ORDER
)
from vnpy.trader.constant import (
    Status, Direction, Offset
)
from vnpy.trader.object import (
    OrderData, OrderRequest, OrderType
)
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.app.option_master.engine import OptionEngine
from vnpy.app.option_master.base import (
    APP_NAME, CHAIN_UNDERLYING_MAP,
    PortfolioData, UnderlyingData, ChainData
)

# if typing.TYPE_CHECKING:
#     from .engine import OptionEngine

class OptionEngineExt(OptionEngine):
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__(main_engine, event_engine)

        self.channel_hedge_engine: ChannelHedgeEngine = ChannelHedgeEngine(self)


class ChannelHedgeEngine:
    def __init__(self, option_engine: OptionEngineExt):
        self.option_engine: OptionEngineExt = option_engine
        self.main_engine: MainEngine = option_engine.main_engine
        self.event_engine: EventEngine = option_engine.event_engine

        self.balance_prices: Dict[str, float] = {}
        self.underlyings: Dict[str, UnderlyingData] = {}
        self.underlying_symbols: Dict[str, str] = {}
        self.synthesis_chain_symbols: Dict[str, str] = {}

        self.auto_portfolio_names: List[str] = []
        
        self.counters: Dict[str, float] = {}

        self.check_delta_trigger: int = 5
        self.calc_balance_trigger: int = 300

        self.chanel_width: float = 0.0
        self.hedge_percent: float = 0.0

        self.balance_price: float = 0.0

        self.init_counter()

    def init_counter(self) -> None:
        self.counters['check_delta'] = 0
        self.counters['calculate_balance'] = 0

    def register_event(self) -> None:
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event: Event) -> None:
        check_delta_counter = self.counters.get('check_delta')
        calc_balance_counter = self.counters.get('calculate_balance')

        if check_delta_counter > self.check_delta_trigger:
            self.check_all_delta()
            check_delta_counter = 0

        if calc_balance_counter > self.calc_balance_trigger:
            self.calc_all_balance()
            calc_balance_counter = 0

        check_delta_counter += 1
        calc_balance_counter += 1

    def set_underlying_symbol(self, portfolio_name: str, underlying_symbol: str):
        self.underlying_symbols[portfolio_name] = underlying_symbol

    def set_synthesis_chain_symbol(self, portfolio_name: str, chain_symbol: str):
        self.synthesis_chain_symbols[portfolio_name] = chain_symbol

    def get_portfolio(self, portfolio_name: str) -> PortfolioData:
        active_portfolios = self.option_engine.active_portfolios
        portfolio = active_portfolios.get(portfolio_name)
        if not portfolio:
            self.write_log(f"通道对冲模块找不到组合{portfolio_name}")
        return portfolio

    def get_underlying(self, portfolio_name: str) -> UnderlyingData:
        underlying = self.underlyings.get(portfolio_name)

        if not underlying:
            portfolio = self.get_portfolio(portfolio_name)
            if not portfolio:
                return

            symbol = self.underlying_symbols.get(portfolio_name)
            if not symbol:
                self.write_log(f"找不到组合{portfolio_name}对应标的代码")
                return
            underlying = portfolio.underlyings.get(symbol)
            if not underlying:
                self.write_log(f"找不到组合{portfolio_name}对应标的{symbol}")
                return
            self.underlyings[portfolio_name] = underlying

        return self.underlyings[portfolio_name]
    
    def get_balance_price(self, portfolio_name: str) -> float:
        price = self.balance_prices.get(portfolio_name)
        if not price:
            self.calculate_balance_price(portfolio_name)
        return price

    def get_synthesis_chain(self, portfolio_name) -> ChainData:
        portfolio = self.get_portfolio(portfolio_name)
        if not portfolio:
            return

        chain_symbol = self.synthesis_chain_symbols.get(portfolio_name)
        chain = portfolio.get_chain(chain_symbol)
        return chain

    def calculate_pos_delta(self, portfolio_name: str, price: float) -> float:
        portfolio = self.get_portfolio(portfolio_name)
        if not portfolio:
            return

        portfolio_delta = 0
        for option in portfolio.options.values():
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
                portfolio_delta += delta
        return portfolio_delta

    def calculate_balance_price(self, portfolio_name: str) -> None:
        underlying = self.get_underlying(portfolio_name)
        if not underlying:
            return

        price = underlying.mid_price
        delta = self.calculate_pos_delta(portfolio_name, price)

        if delta > 0:
            while True:
                last_price = price
                price += price * 0.003
                delta = self.calculate_pos_delta(portfolio_name, price)
                if delta <= 0:
                    balance_price = (last_price + price) / 2
                    self.balance_prices[portfolio_name] = balance_price 
        else:
            while True:
                last_price = price
                price -= price * 0.003
                delta = self.calculate_pos_delta(portfolio_name, price)
                if delta >= 0:
                    balance_price = (last_price + price) / 2
                    self.balance_prices[portfolio_name] = balance_price

    def calc_all_balance(self) -> None:
        for portfolio_name in self.auto_portfolio_names:
            self.calculate_balance_price(portfolio_name)
    
    def check_all_delta(self) -> None:
        for portfolio_name in self.auto_portfolio_names:
            balance_price = self.get_balance_price(portfolio_name)
            up = balance_price * (1 + self.chanel_width)
            down = balance_price * (1 - self.chanel_width)

            underlying = self.get_underlying(portfolio_name)
            if underlying.tick > up:
                self.long_hedge(portfolio_name)
            elif underlying.tick < down:
                self.short_hedge(portfolio_name)
            else:
                continue

    def long_hedge(self, portfolio_name: str):
        chain = self.get_synthesis_chain(portfolio_name)
        atm_call = chain.calls[chain.atm_index]
        atm_put = chain.puts[chain.atm_index]
        unit_hedge_delta = atm_call.cash_delta - atm_put.cash_delta

        portfolio = self.get_portfolio(portfolio_name)
        to_hedge_delta = abs(portfolio.pos_delta) * self.hedge_percent
        to_hedge_volume = round(to_hedge_delta / unit_hedge_delta)

        if not to_hedge_volume:
            self.write_log(f"{portfolio_name} Delta偏移量少于最小对冲单元值")
            return

    def buy(self, volume: int):
        pass

    def short(self, volume: int):
        pass

    def short_hedge(self, portfolio_name: str):
        pass

    def write_log(self, msg: str):
        self.main_engine.write_log(msg, source=APP_NAME)


class ChaseOrderEngine:
    def __init__(self, option_engine: OptionEngineExt):
        self.option_engine: OptionEngineExt = option_engine
        self.main_engine: MainEngine = option_engine.main_engine
        self.event_engine: EventEngine = option_engine.event_engine

        self.active_orderids: Set[str] = set()
        
        self.pay_up: int = 0
        self.cancel_interval: int = 3
        
        self.cancel_counts: Dict[str, int] = {}

        self.register_event()

    def register_event(self) -> None:
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_order_event(self, event: Event) -> None:
        order: OrderData = event.data

        if order.vt_orderid not in self.active_orderids:
            return

        if not order.is_active():
            self.active_orderids.remove(order.vt_orderid)
            self.cancel_counts.pop(order.vt_orderid, None)

        if order.status == Status.CANCELLED:
            self.resend_order(order)

    def process_timer_event(self, event: Event) -> None:
        self.check_cancel()

    def resend_order(self, order: OrderData) -> None:
        new_volume = order.volume - order.traded
        self.send_order(order.vt_symbol, order.direction, order.offset, new_volume)

    def cancel_order(self, vt_orderid: str) -> None:
        """"""
        order = self.main_engine.get_order(vt_orderid)
        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def check_cancel(self) -> None:
        for vt_orderid in self.active_orderids:
            if self.cancel_counts[vt_orderid] > self.cancel_interval:
                self.cancel_order(vt_orderid)
            self.cancel_counts[vt_orderid] += 1

    def send_order(self, vt_symbol: str, direction: Direction, offset: Offset, volume: float, price: Optional[float] = None) -> str:
        contract = self.main_engine.get_contract(vt_symbol)
        tick = self.main_engine.get_tick(vt_symbol)

        if not price:
            if direction == Direction.LONG:
                price = tick.ask_price_1 + contract.pricetick * self.pay_up
            else:
                price = tick.bid_price_1 - contract.pricetick * self.pay_up

        req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            type=OrderType.LIMIT,
            volume=volume,
            price=price
        )

        vt_orderid = self.main_engine.send_order(req, contract.gateway_name)
        self.active_orderids.add(vt_orderid)
        self.cancel_counts[vt_orderid] = 0
        return vt_orderid

    def buy(self, vt_symbol: str, volume: float, price: Optional[float] = None):
        return self.send_order(vt_symbol, Direction.LONG, Offset.OPEN, volume, price)

    def sell(self, vt_symbol: str, volume: float, price: Optional[float] = None):
        return self.send_order(vt_symbol, Direction.SHORT, Offset.CLOSE, volume, price)

    def short(self, vt_symbol: str, volume: float, price: Optional[float] = None):
        return self.send_order(vt_symbol, Direction.SHORT, Offset.OPEN, volume, price)

    def cover(self, vt_symbol: str, volume: float, price: Optional[float] = None):
        return self.send_order(vt_symbol, Direction.LONG, Offset.CLOSE, volume, price)