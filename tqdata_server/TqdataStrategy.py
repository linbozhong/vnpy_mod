from vnpy.trader.constant import Interval
from vnpy.trader.utility import extract_vt_symbol
from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)

EVENT_TQDATA_BAR = "eTqdataBar"

class TqdataStrategy(CtaTemplate):
    author = "demo"

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        self.bg = BarGenerator(self.on_bar)
        # self.am = ArrayManager()

        self.bar_type = 'index'
        self.vt_tq_symbol = f"{self.vt_symbol}.{self.bar_type}"

        self.register_event()

    def register_event(self):
        """"""
        self.cta_engine.event_engine.register(EVENT_TQDATA_BAR + self.vt_tq_symbol, self.process_tqbar)

    def process_tqbar(self, event):
        """"""
        bar = event.data
        self.on_tq_bar(bar)

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")
        self.load_tq_bar(Interval.MINUTE)

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")
        self.put_event()

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

        self.put_event()

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        pass

    def load_tq_bar(self, interval: Interval, size: int = 200):
        """
        Query history bar from tqdata gateway.
        """
        gateway = self.cta_engine.main_engine.get_gateway('Tqdata')
        if gateway:
            gateway.get_bar(self.vt_symbol, self.bar_type, interval, size)
        else:
            return ""

    def on_tq_bar(self, bar: BarData):
        """
        Callback of new tq bar data update.
        """
        self.write_log(f"{bar.datetime} {bar.vt_symbol} {bar.open_price} {bar.high_price} {bar.low_price} {bar.close_price}")

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass
