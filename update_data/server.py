import json
import traceback
import signal
import logging
from typing import List, Callable
from pathlib import Path
from datetime import datetime
from multiprocessing.connection import Listener
from multiprocessing.context import AuthenticationError
from threading import Thread

from vnpy.trader.database import database_manager
from vnpy.trader.utility import load_json, extract_vt_symbol
from vnpy.trader.object import BarData
from vnpy.trader.constant import Interval, Exchange

signal.signal(signal.SIGINT, signal.SIG_DFL)

CTA_SETTING_FILENAME = "cta_strategy_setting.json"

INTERVAL_RQ2VT = {
    "1m": Interval.MINUTE,
    "60m": Interval.HOUR,
    "1d": Interval.DAILY,
}


class Logger(object):
    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s: %(message)s"
        )

        self.logger.setLevel(level)
        self.add_file_handler()
        self.add_console_handler()

    def __getattr__(self, item):
        return getattr(self.logger, item)

    def add_file_handler(self):
        today_date = datetime.now().strftime("%Y%m%d")
        filename = f"{self.name}_run.log"
        file_path = Path.cwd().joinpath(filename)

        file_handler = logging.FileHandler(
            file_path, mode="a", encoding="utf8"
        )
        file_handler.setLevel(self.level)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

    def add_console_handler(self):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)


logger = Logger("data_updater")


class RpcServer:
    def __init__(self):
        self._functions = {}

    def register(self, func: Callable):
        self._functions[func.__name__] = func

    def handle_connection(self, connection):
        try:
            while True:
                func_name, args, kwargs = json.loads(connection.recv())
                try:
                    r = self._functions[func_name](*args, **kwargs)
                    rep = (True, r)
                    connection.send(json.dumps(rep))
                    logger.info(f"{func_name}运行成功")
                except:
                    msg = traceback.format_exc()
                    rep = (False, msg)
                    connection.send(json.dumps(rep))
                    logger.error(msg)
        except EOFError:
            pass
        except Exception as e:
            logger.error(e)


class DataRpcServer(RpcServer):
    def __init__(self):
        super(DataRpcServer, self).__init__()
        self.host = "0.0.0.0"
        self.port = 17000
        self.authkey = b"keykey"

        self.init_server()

    def init_server(self):
        self.register(self.connect_test)
        self.register(self.echo_test)
        self.register(self.get_update_symbol)
        self.register(self.save_to_database)

    def run_server(self):
        sock = Listener(address=(self.host, self.port), authkey=self.authkey)
        while 1:
            logger.info("开始服务..")
            try:
                connection = sock.accept()
                th = Thread(target=self.handle_connection, args=(connection,))
                th.daemon = True
                th.start()
            except AuthenticationError as e:
                logger.error(e)
            except Exception as e:
                logger.error(e)

    @staticmethod
    def get_update_symbol() -> List:
        data = load_json(CTA_SETTING_FILENAME)
        symbols = set()
        for _name, setting in data.items():
            symbols.add(setting['vt_symbol'])
        return list(symbols)

    @staticmethod
    def save_to_database(data: List[dict], vt_symbol: str, rq_interval: str):
        interval = INTERVAL_RQ2VT.get(rq_interval)
        if not rq_interval:
            return None

        symbol, exchange = extract_vt_symbol(vt_symbol)
        exchange = Exchange(exchange)
        dt_format = "%Y-%m-%d %H:%M:%S"

        res_list: List[BarData] = []
        if data is not None:
            for row in data:
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=datetime.strptime(row['datetime'], dt_format),
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["volume"],
                    gateway_name="RQ_WEB"
                )
                res_list.append(bar)
        database_manager.save_bar_data(res_list)

    @staticmethod
    def connect_test():
        return "连接成功"

    @staticmethod
    def echo_test(value):
        return value


if __name__ == '__main__':
    server = DataRpcServer()

    server.run_server()
    # print(server.get_update_symbol())
