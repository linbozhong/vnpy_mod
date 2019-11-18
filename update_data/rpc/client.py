import json
from multiprocessing.connection import Client


class RemoteException(Exception):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        return self.__value


class RpcClient:
    def __init__(self):
        self._connection = None

        self.host = "localhost"
        self.port = 17000
        self.authkey = b"keykey"

    def connect(self):
        self._connection = Client(address=(self.host, self.port), authkey=self.authkey)
        rep = self.connect_test()
        print(rep)

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


if __name__ == "__main__":
    client = RpcClient()
    client.connect()


    def echo_test(*args, **kwargs):
        res = client.echo_test(*args, **kwargs)
        print(type(res), res)


    echo_test("hello")
    echo_test(123)
    echo_test([1, 2, 2])
    echo_test({'a': 1})
