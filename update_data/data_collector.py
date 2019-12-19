import traceback
import socket
import pandas as pd
import time

from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, Sequence, List
from rpc.client import RpcClient
from rpc.utility import INTERVAL_ADJUSTMENT_MAP
from rpc.utility import (get_duration, extract_vt_symbol, to_rq_symbol, handle_df,
                         ts_to_dt, strip_digt,
                         load_json, save_json)


def get_trading_symbols() -> set:
    symbols = set()
    df = all_instruments(date=datetime.now())
    for ix, row in df.iterrows():
        symbols.add(row["order_book_id"])
    return symbols


def get_exchange_map() -> pd.Series:
    df = all_instruments(type='Future', date=datetime.now())
    df.drop_duplicates(subset='underlying_symbol', inplace=True)
    df.set_index('underlying_symbol', drop=True, inplace=True)
    return df['exchange']


def to_vt_symbol(rq_symbol: str) -> str:
    exchange = exchange_dict[strip_digt(rq_symbol).upper()]
    return f"{rq_symbol}.{exchange}"


def get_data_by_month(rq_symbol: str, rq_interval: str, start_date: datetime, end_date: datetime) -> dict:
    # 没有加时间的dt，rq默认截止到上个收盘点，加1天可以截止到当前时间或加当日夜盘数据。
    end_date += timedelta(1)

    df = get_price(
        rq_symbol,
        frequency=rq_interval,
        fields=["open", "high", "low", "close", "volume"],
        start_date=start_date,
        end_date=end_date,
        adjust_type="none"
    )

    df = handle_df(df, rq_interval)
#     return df
    return df.to_dict(orient="records")


def gen_start_end_pair(start: datetime, end: datetime) -> List[tuple]:
    """
    生成月的开始结束对
    """
    ends = pd.date_range(start=start, end=end, freq='M')
    starts = ends.shift(-1)
    starts = map(lambda ts: ts_to_dt(ts) + timedelta(1), starts)
    pairs = list(zip(starts, map(ts_to_dt, ends)))
    return pairs


def init_client(host: str, port: int, authkey: bytes):
    client = RpcClient(host, port, authkey)
    client.connect()
    return client


def save_all_data(client: RpcClient, rq_interval: str, start: datetime, end: datetime, symbols: Optional[Sequence[str]] = None):
    if symbols is None:
        #         symbols = get_update_symbol(client)
        print('without mission')
    for rq_symbol in symbols:
        pairs = gen_start_end_pair(start, end)
        vt_symbol = to_vt_symbol(rq_symbol)
        collected = collected_dict.get(vt_symbol, [])
        for (s, e) in pairs:
            try:
                flag_name = s.strftime("%Y%m")
                if flag_name in collected:
                    print(f"{vt_symbol}-{flag_name}数据已存在")
                    continue

                data_dict = get_data_by_month(rq_symbol, rq_interval, s, e)
                client.save_to_database(data_dict, vt_symbol, rq_interval)
                print(f"{vt_symbol}-{flag_name}数据保存成功")

                collected_dict.setdefault(rq_symbol, []).append(flag_name)
                print("休息2秒")
                time.sleep(2)
            except:
                traceback.print_exc()


collected_dict = load_json('collected.json')
exchange_dict = get_exchange_map()
