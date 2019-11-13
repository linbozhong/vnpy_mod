# coding: utf-8

import logging
from time import sleep
from datetime import datetime
from pathlib import Path

from .setting import logger_level


class Logger(object):
    def __init__(self, level=logging.INFO):
        self.logger = logging.getLogger("sync_logger")
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
        filename = f"sync_{today_date}.log"
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


logger = Logger(logger_level)

if __name__ == "__main__":
    logger = Logger(level=logging.DEBUG)


    def log_msg():
        logger.debug('debug')
        logger.info("hello world")


    while 1:
        log_msg()
        sleep(3)
