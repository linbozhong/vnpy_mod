import zmq
import time
import threading
from zmq.backend.cython.constants import NOBLOCK
from vnpy.trader.event import EVENT_ACCOUNT

KEEP_ALIVE_TOPIC = "_keep_alive"


if __name__ == "__main__":
    zmq_context = zmq.Context()
    zmq_req = zmq_context.socket(zmq.REQ)
    zmq_sub = zmq_context.socket(zmq.SUB)

    zmq_sub.setsockopt_string(zmq.SUBSCRIBE, "")

    zmq_req.connect("tcp://127.0.0.1:2014")
    zmq_sub.connect("tcp://127.0.0.1:4102")

    print("等待5秒，连接RPC接口")
    time.sleep(5)
    print("等待完成")

    while True:
        if not zmq_sub.poll(3000):
            print('nothing happen')
            continue

        topic, data = zmq_sub.recv_pyobj(flags=NOBLOCK)

        if topic == KEEP_ALIVE_TOPIC:
            continue

        if data is None:
            print("None data", topic, data)
            continue
        else:
            if data.type == EVENT_ACCOUNT:
                account_data = data.data
                account = {
                    'accountid': account_data.accountid,
                    'balance': account_data.balance
                }

                print(account)