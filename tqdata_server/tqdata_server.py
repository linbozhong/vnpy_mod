import tqsdk
import pandas as pd

from time import sleep
from tqsdk import TqApi
from datetime import datetime
from typing import Any, Union, Optional

from vnpy.event import Event, EventEngine, EVENT_TIMER
from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.utility import extract_vt_symbol
from rpc import RpcServer, KEEP_ALIVE_TOPIC

EVENT_TQDATA_BAR = "eTqdataBar"

INTERVAL_MAP_VT2TQ = {
    Interval.MINUTE: 60,
    Interval.HOUR: 3600,
    Interval.DAILY: 86400
} 

def strip_digt(symbol: str) -> str:
    res = ""
    for char in symbol:
        if not char.isdigit():
            res += char
        else:
            break
    return res

def vt_symbol_to_tq_symbol(vt_symbol: str, bar_type: str):
    """
    bar_type: "trading", "index", "main"
    """
    symbol, exchange = extract_vt_symbol(vt_symbol)
    if bar_type == "trading":
        return f"{exchange.value}.{symbol}"
    elif bar_type == "index":
        return f"KQ.i@{exchange.value}.{strip_digt(symbol)}"
    elif bar_type == "main":
        return f"KQ.m@{exchange.value}.{strip_digt(symbol)}"
    else:
        raise ValueError("The bar_type argument must be trading, index or main")


class TqdataServer():

    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.event_engine.start()

        self.rep_address = "tcp://*:12914"
        self.pub_address = "tcp://*:41921"

        self.rpc_server = RpcServer()
        self.rpc_server.register(self.get_bar)
        self.rpc_server.start(self.rep_address, self.pub_address)

        self.tqapi = TqApi()
        self.data_dict = {}

        self.register_event()

    @staticmethod
    def parse_bar_name(bar_name: str):
        vt_symbol, bar_type, interval = bar_name.split('_')
        interval = Interval(interval)
        _symbol, exchange = extract_vt_symbol(vt_symbol)
        vt_tq_symbol = f"{vt_symbol}.{bar_type}"
        return vt_tq_symbol, exchange, interval

    @staticmethod
    def to_vt_bar(data: Union[dict, pd.Series], symbol: str, exchange: Exchange, interval: Interval) -> BarData:
        bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=datetime.fromtimestamp(data["datetime"] / 1e9),
                    open_price=data["open"],
                    high_price=data["high"],
                    low_price=data["low"],
                    close_price=data["close"],
                    volume=data["volume"],
                    gateway_name="Tqdata"
                )
        return bar

    def init_all_commodity():
        pass


    def get_bar(self, vt_symbol: str, bar_type: str, interval: Interval, size: int = 200):
        print(vt_symbol, bar_type, interval, size)
        vt_tq_symbol = f"{vt_symbol}.{bar_type}"
        _, exchange = extract_vt_symbol(vt_symbol)
        tq_interval = INTERVAL_MAP_VT2TQ.get(interval, None)
        if tq_interval is None:
            raise KeyError("The interval can only be daily, hour or minute")
        bar_name = f"{vt_symbol}_{bar_type}_{interval.value}"
        bars_df = self.data_dict.get(bar_name, None)
        if bars_df is None:
            tq_symbol = vt_symbol_to_tq_symbol(vt_symbol, bar_type)
            print('get_bar_arguments', tq_symbol, tq_interval, size)
            bars_df = self.tqapi.get_kline_serial(tq_symbol, tq_interval, size)
            self.data_dict[bar_name] = bars_df
            print(bars_df)

        for _ix, row in bars_df.iterrows():
            vt_bar = self.to_vt_bar(row, vt_tq_symbol, exchange, interval)
            self.on_tqdata_bar(vt_bar)

    def register_event(self):
        self.event_engine.register_general(self.process_event)

    def process_event(self, event: Event):
        if event.type == EVENT_TIMER:
            self.rpc_server.publish(KEEP_ALIVE_TOPIC, datetime.now())
        else:
            self.rpc_server.publish("", event)

    def on_event(self, type_: str, data: Any):
        event = Event(type_, data)
        self.event_engine.put(event)

    def on_tqdata_bar(self, bar):
        self.on_event(EVENT_TQDATA_BAR, bar)
        self.on_event(EVENT_TQDATA_BAR + bar.symbol, bar)

    def start(self):
        while True:
            self.tqapi.wait_update()
            for bar_name, bar in self.data_dict.items():
                if self.tqapi.is_changing(bar.iloc[-1], "datetime"):
                    vt_tq_symbol, exchange, interval = self.parse_bar_name(bar_name)
                    self.on_tqdata_bar(self.to_vt_bar(bar.iloc[-1], vt_tq_symbol, exchange, interval))


if __name__ == "__main__":
    event_engine = EventEngine()
    publisher = TqdataServer(event_engine)
    # publisher.get_bar('IF2005.CFFEX', 'index', Interval.MINUTE, 300)
    # publisher.start()
    # print(vt_symbol_to_tq_symbol('rb2010.SHFE', 'index'))

    # tqapi = TqApi()
    # df = tqapi.get_kline_serial('KQ.i@CFFEX.IF', 60, 200)
    # print(df)