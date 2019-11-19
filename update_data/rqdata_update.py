import traceback
from datetime import datetime, timedelta
from typing import Optional, Sequence, List
from rpc.client import RpcClient
from rpc.utility import INTERVAL_ADJUSTMENT_MAP
from rpc.utility import get_duration, extract_vt_symbol, to_rq_symbol, handle_df


def get_trading_symbols() -> set:
    symbols = set()
    df = all_instruments(date=datetime.now())
    for ix, row in df.iterrows():
        symbols.add(row["order_book_id"])
    return symbols


def query_by_symbol(vt_symbol: str, rq_interval: str, start_date: datetime, end_date: datetime) -> dict:
    symbol, exchange = extract_vt_symbol(vt_symbol)
    rq_symbol = to_rq_symbol(symbol, exchange)
    adjustment = INTERVAL_ADJUSTMENT_MAP[rq_interval]
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
    return df.to_dict(orient="records")


def init_client(host: str, port: int, authkey: bytes):
    client = RpcClient(host, port, authkey)
    try:
        client.connect()
        return client
    except:
        traceback.print_exc()


def get_update_symbol(client: RpcClient) -> List:
    symbols = client.get_update_symbol()
    print("待更新的合约列表获取成功：")
    print(symbols)
    return symbols


def save_all_data(client: RpcClient, rq_interval: str, start: datetime, end: datetime,
                  symbols: Optional[Sequence[str]] = None):
    if symbols is None:
        symbols = get_update_symbol(client)
    for vt_symbol in symbols:
        data_dict = query_by_symbol(vt_symbol, rq_interval, start, end)
        client.save_to_database(data_dict, vt_symbol, rq_interval)
        print(f"{vt_symbol}合约数据保存成功")