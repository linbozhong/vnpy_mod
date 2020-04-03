import signal
import threading
import traceback
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Callable

import zmq

_ = lambda x: x

# Achieve Ctrl-c interrupt recv
from zmq.backend.cython.constants import NOBLOCK

signal.signal(signal.SIGINT, signal.SIG_DFL)

KEEP_ALIVE_TOPIC = '_keep_alive'
KEEP_ALIVE_INTERVAL = timedelta(seconds=1)
KEEP_ALIVE_TOLERANCE = timedelta(seconds=3)


class RemoteException(Exception):
    """
    RPC remote exception
    """

    def __init__(self, value):
        """
        Constructor
        """
        self.__value = value

    def __str__(self):
        """
        Output error message
        """
        return self.__value


class RpcServer:
    """"""

    def __init__(self):
        """
        Constructor
        """
        print('RpcServer move all pub to event process thread')

        # Save functions dict: key is fuction name, value is fuction object
        self.__functions = {}

        # Zmq port related
        self.__context = zmq.Context()

        # Reply socket (Request–reply pattern)
        self.__socket_rep = self.__context.socket(zmq.REP)

        # Publish socket (Publish–subscribe pattern)
        self.__socket_pub = self.__context.socket(zmq.PUB)

        # Worker thread related
        self.__active = False                               # RpcServer status
        self.__thread = None                                # RpcServer thread

        # self.__lock = threading.Lock()

        self._register(KEEP_ALIVE_TOPIC, lambda n: n)

    def is_active(self):
        """"""
        return self.__active

    def start(self, rep_address: str, pub_address: str):
        """
        Start RpcServer
        """
        if self.__active:
            return

        # Bind socket address
        self.__socket_rep.bind(rep_address)
        self.__socket_pub.bind(pub_address)

        # Start RpcServer status
        self.__active = True

        # Start RpcServer thread
        self.__thread = threading.Thread(target=self.run)
        self.__thread.start()

        print("RPC服务已启动")

    def stop(self):
        """
        Stop RpcServer
        """
        if not self.__active:
            return

        # Stop RpcServer status
        self.__active = False

        # Unbind socket address
        self.__socket_pub.unbind(self.__socket_pub.LAST_ENDPOINT)
        self.__socket_rep.unbind(self.__socket_rep.LAST_ENDPOINT)

    def join(self):
        # Wait for RpcServer thread to exit
        if self.__thread.isAlive():
            self.__thread.join()
        self.__thread = None

    def run(self):
        """
        Run RpcServer functions
        """
        while self.__active:
            if not self.__socket_rep.poll(1000):
                continue

            # Receive request data from Reply socket
            req = self.__socket_rep.recv_pyobj()

            # Get function name and parameters
            name, args, kwargs = req

            # Try to get and execute callable function object; capture exception information if it fails
            try:
                func = self.__functions[name]
                r = func(*args, **kwargs)
                rep = [True, r]
            except Exception as _:  # noqa
                rep = [False, traceback.format_exc()]

            # send callable response by Reply socket
            self.__socket_rep.send_pyobj(rep)

    def publish(self, topic: str, data: Any):
        """
        Publish data
        """
        # print("Threading:", threading.currentThread())
        # with self.__lock:
        self.__socket_pub.send_pyobj([topic, data])

    def register(self, func: Callable):
        """
        Register function
        """
        return self._register(func.__name__, func)

    def _register(self, name: str, func: Callable):
        """
        Register function
        """
        self.__functions[name] = func