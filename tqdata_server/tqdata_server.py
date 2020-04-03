import tqsdk
import pandas as pd

from time import sleep
from tqsdk import TqApi
from datetime import datetime
from typing import Any, Union

from vnpy.event import Event, EventEngine, EVENT_TIMER
from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval
from rpc import RpcServer, KEEP_ALIVE_TOPIC

EVENT_TQDATA_BAR = "eTqdataBar"


class TqdataServer():

    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.event_engine.start()

        self.rep_address = "tcp://*:12914"
        self.pub_address = "tcp://*:41921"

        self.rpc_server = RpcServer()
        self.rpc_server.start(self.rep_address, self.pub_address)

        self.tqapi = TqApi()
        self.data_list = []

        for commodity in ['IF', 'IC', 'IH']:
        # for commodity in ['bu', 'rb', 'cu', 'IF']:
            # bar = self.tqapi.get_kline_serial(f"KQ.i@SHFE.{commodity}", 5)
            bar = self.tqapi.get_kline_serial(f"KQ.i@CFFEX.{commodity}", 5)
            self.data_list.append(bar)

        self.register_event()

    @staticmethod
    def to_bar(data: Union[dict, pd.Series]) -> BarData:
        bar = BarData(
                    symbol=data['symbol'],
                    exchange=Exchange('CFFEX'),
                    interval=Interval('d'),
                    datetime=datetime.fromtimestamp(data["datetime"] / 1e9),
                    open_price=data["open"],
                    high_price=data["high"],
                    low_price=data["low"],
                    close_price=data["close"],
                    volume=data["volume"],
                    gateway_name="Tqdata"
                )
        return bar
    
    def register_event(self):
        self.event_engine.register_general(self.process_event)

    def process_event(self, event: Event):
        if event.type == EVENT_TIMER:
            self.rpc_server.publish(KEEP_ALIVE_TOPIC, datetime.now())
        else:
            self.rpc_server.publish("", event)

    def on_event(self, type: str, data: Any):
        event = Event(type, data)
        self.event_engine.put(event)

    def on_tqdata_bar(self, bar):
        self.on_event(EVENT_TQDATA_BAR, bar)
        self.on_event(EVENT_TQDATA_BAR + bar.symbol, bar)

    def start(self):
        while True:
            self.tqapi.wait_update()

            for bar in self.data_list:
                if self.tqapi.is_changing(bar.iloc[-1], "datetime"):
                    # print(bar.iloc[-1])
                    print(datetime.fromtimestamp(bar.iloc[-1]["datetime"] / 1e9), bar.iloc[-1]['close'], bar.iloc[-1]['symbol'])
                    self.on_tqdata_bar(self.to_bar(bar.iloc[-1]))


if __name__ == "__main__":
    event_engine = EventEngine()
    publisher = TqdataServer(event_engine)
    publisher.start()