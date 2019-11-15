import logging
from enum import Enum


class Content(Enum):
    CTA_DATA = "cta_data"
    CTA_SETTING = "cta_setting"
    FOLLOW_DATA = "follow_data"


CTA_DATA_FILENAME = "cta_strategy_data.json"
CTA_SETTING_FILENAME = "cta_strategy_setting.json"
FOLLOW_DATA_FILENAME = "follow_trading_data.json"

MYSQL_SETTING_FILENAME = "mysql_setting.json"

run_mod = "production"

if run_mod == "production":
    cta_dir = "E:\\vnpy\\vnpy-2.0.7\\examples\\cta_trader\\.vntrader"
    follow_dir = "E:\\vnpy\\vnpy-2.0.7\\examples\\follower_trader\\.vntrader"
else:
    cta_dir = ""
    follow_dir = ""

file_setting = {
    CTA_DATA_FILENAME: cta_dir,
    CTA_SETTING_FILENAME: cta_dir,
    FOLLOW_DATA_FILENAME: follow_dir
}

mysql_setting = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "user",
    "password": "password",
    "database": "database_nmae"
}

logger_level = logging.INFO
