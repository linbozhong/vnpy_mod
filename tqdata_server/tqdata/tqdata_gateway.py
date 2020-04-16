from vnpy.event import Event
from vnpy.rpc import RpcClient
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    SubscribeRequest,
    CancelRequest,
    OrderRequest
)
from vnpy.trader.constant import Exchange, Interval


class TqdataGateway(BaseGateway):
    """
    Tqdata Gateway.
    """

    default_setting = {
        "主动请求地址": "tcp://127.0.0.1:12914",
        "推送订阅地址": "tcp://127.0.0.1:41921"
    }

    exchanges = list(Exchange)

    def __init__(self, event_engine):
        """Constructor"""
        super().__init__(event_engine, "Tqdata")

        self.symbol_gateway_map = {}

        self.client = RpcClient()
        self.client.callback = self.client_callback

    def connect(self, setting: dict):
        """"""
        req_address = setting["主动请求地址"]
        pub_address = setting["推送订阅地址"]

        self.client.subscribe_topic("")
        self.client.start(req_address, pub_address)

        self.write_log("服务器连接成功，开始初始化查询")

        self.query_all()

    def subscribe(self, req: SubscribeRequest):
        """"""
        pass

    def get_bar(self, vt_symbol: str, bar_type: str, interval: Interval, size: int = 200):
        """"""
        self.client.get_bar(vt_symbol, bar_type, interval, size)
        print(vt_symbol, bar_type, interval, size)
        print('histroy request sended.')

    def start_tq_pub(self):
        """"""
        self.client.start_tq_pub()

    def send_order(self, req: OrderRequest):
        """"""
        pass

    def cancel_order(self, req: CancelRequest):
        """"""
        pass

    def query_account(self):
        """"""
        pass

    def query_position(self):
        """"""
        pass

    def query_all(self):
        """"""
        pass

    def close(self):
        """"""
        self.client.stop()
        self.client.join()

    def client_callback(self, topic: str, event: Event):
        """"""
        if event is None:
            print("none event", topic, event)
            return

        data = event.data

        # print(event.type)
        # print(data)

        if hasattr(data, "gateway_name"):
            data.gateway_name = self.gateway_name

        self.event_engine.put(event)
