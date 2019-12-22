import json
import traceback
from multiprocessing.connection import Client


class RemoteException(Exception):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        return self.__value


class RpcClient:
    def __init__(self, host: str = "localhost", port: int = 17000, authkey: bytes = b"keykey"):
        self._connection = None

        self.host = host
        self.port = port
        self.authkey = authkey

    def connect(self):
        try:
            self._connection = Client(address=(self.host, self.port), authkey=self.authkey)
            rep = self.connect_test()
            print(rep)
        except:
            if self._connection:
                self.close()
            traceback.print_exc()

    def close(self):
        self._connection.close()
        print("连接关闭")

    def __getattr__(self, name):
        def do_rpc(*args, **kwargs):
            self._connection.send(json.dumps((name, args, kwargs)))
            rep = json.loads(self._connection.recv())

            if rep[0]:
                return rep[1]
            else:
                self.close()
                raise RemoteException(rep[1])

        return do_rpc