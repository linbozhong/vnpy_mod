from typing import Tuple
from datetime import datetime, timedelta
from enum import Enum
from pandas import DataFrame, Timestamp
from copy import copy


class Exchange(Enum):
    """
    Exchange.
    """
    # Chinese
    CFFEX = "CFFEX"  # China Financial Futures Exchange
    SHFE = "SHFE"  # Shanghai Futures Exchange
    CZCE = "CZCE"  # Zhengzhou Commodity Exchange
    DCE = "DCE"  # Dalian Commodity Exchange
    INE = "INE"  # Shanghai International Energy Exchange
    SSE = "SSE"  # Shanghai Stock Exchange
    SZSE = "SZSE"  # Shenzhen Stock Exchange
    SGE = "SGE"  # Shanghai Gold Exchange
    WXE = "WXE"  # Wuxi Steel Exchange


INTERVAL_ADJUSTMENT_MAP = {
    "1m": timedelta(minutes=1),
    "60m": timedelta(hours=1),
    "1d": timedelta()  # no need to adjust for daily bar
}


def ts_to_str(ts: Timestamp) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def get_duration(days: int = 20) -> Tuple[datetime, datetime]:
    end = datetime.now()
    start = end - timedelta(days)
    return start, end


def is_hour_datetime(dt: datetime) -> bool:
    minute = dt.minute
    return minute == 0 or minute == 30


def extract_vt_symbol(vt_symbol: str) -> Tuple[str, Exchange]:
    symbol, exchange_str = vt_symbol.split(".")
    return symbol, Exchange(exchange_str)


def to_rq_symbol(symbol: str, exchange: Exchange) -> str:
    """
    CZCE product of RQData has symbol like "TA1905" while
    vt symbol is "TA905.CZCE" so need to add "1" in symbol.
    """
    if exchange in [Exchange.SSE, Exchange.SZSE]:
        if exchange == Exchange.SSE:
            rq_symbol = f"{symbol}.XSHG"
        else:
            rq_symbol = f"{symbol}.XSHE"
    else:
        if exchange is not Exchange.CZCE:
            return symbol.upper()

        for count, word in enumerate(symbol):
            if word.isdigit():
                break

        # Check for index symbol
        time_str = symbol[count:]
        if time_str in ["88", "888", "99"]:
            return symbol

        # noinspection PyUnboundLocalVariable
        product = symbol[:count]
        year = symbol[count]
        month = symbol[count + 1:]

        if year == "9":
            year = "1" + year
        else:
            year = "2" + year

        rq_symbol = f"{product}{year}{month}".upper()

    return rq_symbol


def handle_df(df: DataFrame, rq_interval: str) -> DataFrame:
    adjustment = INTERVAL_ADJUSTMENT_MAP[rq_interval]

    df["datetime"] = df.index
    df["datetime"] = df["datetime"] - adjustment

    mask = df.datetime.map(is_hour_datetime)
    df = df[mask]

    df = copy(df)
    df["datetime"] = df["datetime"].map(ts_to_str)

    return df
