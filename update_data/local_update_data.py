import json
import traceback
import signal
from typing import List, Callable, Optional, Sequence
from pathlib import Path
from datetime import datetime, timedelta
from opendatatools import futures
from pandas import DataFrame, Timestamp

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

INTERVAL_ADJUSTMENT_MAP = {
    "1m": timedelta(minutes=1),
    "60m": timedelta(hours=1),
    "1d": timedelta()         # no need to adjust for daily bar
}


def load_local_json(filename: str) -> dict:
    filepath = Path.cwd().joinpath(filename)

    if filepath.exists():
        with open(filepath, mode="r", encoding="UTF-8") as f:
            data = json.load(f)
        return data
    else:
        save_local_json(filename, {})
        return {}


def save_local_json(filename: str, data) -> None:
    filepath = Path.cwd().joinpath(filename)
    with open(filepath, mode="w+", encoding="UTF-8") as f:
        json.dump(
            data,
            f,
            indent=4,
            ensure_ascii=False
        )


def to_rq_symbol(symbol: str, exchange: Exchange) -> str:
    """
    CZCE product of RQData has symbol like "TA1905" while
    vt symbol is "TA905.CZCE" so need to add "1" in symbol.
    """
    # Equity
    if exchange in [Exchange.SSE, Exchange.SZSE]:
        if exchange == Exchange.SSE:
            rq_symbol = f"{symbol}.XSHG"
        else:
            rq_symbol = f"{symbol}.XSHE"
    # Spot
    elif exchange in [Exchange.SGE]:
        for char in ["(", ")", "+"]:
            symbol = symbol.replace(char, "")
        symbol = symbol.upper()
        rq_symbol = f"{symbol}.SGEX"
    # Futures and Options
    elif exchange in [Exchange.SHFE, Exchange.CFFEX, Exchange.DCE, Exchange.CZCE, Exchange.INE]:
        for count, word in enumerate(symbol):
            if word.isdigit():
                break

        product = symbol[:count]
        time_str = symbol[count:]

        # Futures
        if time_str.isdigit():
            if exchange is not Exchange.CZCE:
                return symbol.upper()

            # Check for index symbol
            if time_str in ["88", "888", "99"]:
                return symbol

            year = symbol[count]
            month = symbol[count + 1:]

            if year == "9":
                year = "1" + year
            else:
                year = "2" + year

            rq_symbol = f"{product}{year}{month}".upper()
        # Options
        else:
            if exchange in [Exchange.CFFEX, Exchange.DCE, Exchange.SHFE]:
                rq_symbol = symbol.replace("-", "").upper()
            elif exchange == Exchange.CZCE:
                year = symbol[count]
                suffix = symbol[count + 1:]

                if year == "9":
                    year = "1" + year
                else:
                    year = "2" + year

                rq_symbol = f"{product}{year}{suffix}".upper()
    else:
        rq_symbol = f"{symbol}.{exchange.value}"

    return rq_symbol


def ts_to_str(ts: Timestamp) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def is_hour_datetime(dt: datetime) -> bool:
    minute = dt.minute
    return minute == 0 or minute == 30


def handle_df(df: DataFrame, rq_interval: str, need_adjust: bool = True) -> DataFrame:
    adjustment = INTERVAL_ADJUSTMENT_MAP[rq_interval]

    df["datetime"] = df.index
    if need_adjust:
        df["datetime"] = df["datetime"] - adjustment

    if rq_interval == "60m":
        mask = df.datetime.map(is_hour_datetime)
        df = df[mask].copy()
#     df = copy(df)
    df["datetime"] = df["datetime"].map(ts_to_str)
    return df


def get_update_symbol() -> List:
    data = load_json(CTA_SETTING_FILENAME)
    symbols = set()
    for _name, setting in data.items():
        symbols.add(setting['vt_symbol'])
    return list(symbols)


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


def delete_bar_data(symbol: str, exchange: str, interval: str) -> int:
    interval = INTERVAL_RQ2VT[interval]
    exchange = Exchange(exchange)
    return database_manager.delete_bar_data(symbol, exchange, interval)


def clean_data_by_symbol(symbol: str):
    database_manager.clean(symbol)


def query_by_symbol(vt_symbol: str, source_interval: str) -> dict:
    # symbol convert rules of opendatatools is same with rqdata
    # opendatatools can only fetch 30 days data recently, so it dosen't need to specified start and end date
    symbol, exchange = extract_vt_symbol(vt_symbol)
    # print(vt_symbol,symbol, exchange)
    rq_symbol = to_rq_symbol(symbol, exchange)

    df, msg = futures.get_kline(source_interval, rq_symbol)
    df['datetime'] = df['datetime'].map(
        lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df.set_index('datetime', inplace=True, drop=True)
    df = df[::-1].copy()

    df = handle_df(df, source_interval)
#     return df
    return df.to_dict(orient="records")


def save_all_data(source_interval: str, symbols: Optional[Sequence[str]] = None):
    if symbols is None:
        symbols = get_update_symbol()
    for vt_symbol in symbols:
        data_dict = query_by_symbol(vt_symbol, source_interval)
        save_to_database(data_dict, vt_symbol, source_interval)
        print(f"{vt_symbol}合约数据保存成功")


def save_all_data_from_rqdata_json():
    downloaded_data = load_local_json("daily_update_bars.json")
    for vt_symbol, value in downloaded_data.items():
        source_interval = value['rq_interval']
        data_dict = value['data']
        save_to_database(data_dict, vt_symbol, source_interval)
        print(f"{vt_symbol}合约数据保存成功")


def delete_all_bar(source_interval: str, symbols: Optional[Sequence[str]] = None):
    # bar number is different. so if switch data source, it need delete old data.
    if symbols is None:
        symbols = get_update_symbol()
    for vt_symbol in symbols:
        symbol, exchange = extract_vt_symbol(vt_symbol)
        clean_data_by_symbol(symbol)
#         count = client.delete_bar_data(symbol, exchange.value, source_interval)
        print(f"{vt_symbol}合约数据删除成功")


if __name__ == '__main__':
    # get data from opendatatools
#     source_interval = "60m"
#     symbols = get_update_symbol()
#     print(symbols)
# #         symbols = ['ni2105.SHFE']
#     save_all_data(source_interval, symbols)


    # get data from rqdata downloaded json
    save_all_data_from_rqdata_json()
