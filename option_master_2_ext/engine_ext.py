import typing

from typing import Optional
from vnpy.event import EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.app.option_master.engine import OptionEngine
from vnpy.app.option_master.base import PortfolioData

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

        self.portfolio_name: str = ""
        self.portfolio: Optional[PortfolioData] = None 

        self.vt_symbol: str = ""

        self.timer_trigger = 5
        self.balance_price = 0.0

    def get_portfolio(self):
        if not self.portfolio:
            self.portfolio = self.option_engine.get_portfolio(self.portfolio_name)
        return self.portfolio
            
    def calculate_pos_delta(self, price: float):
        portfolio = self.get_portfolio()
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


    def calculate_balance_price(self):
        underlying = self.get_portfolio().underlyings.get(self.vt_symbol)
        price = underlying.mid_price
        delta = self.calculate_pos_delta(price)

        if delta > 0:
            while True:
                last_price = price
                price += price * 0.003
                delta = self.calculate_pos_delta(price)
                if delta <= 0:
                    self.balance_price = (last_price + price) / 2
        else:
            while True:
                last_price = price
                price -= price * 0.003
                delta = self.calculate_pos_delta(price)
                if delta >= 0:
                    self.balance_price = (last_price + price) / 2
                





