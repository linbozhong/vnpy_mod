# 目前米筐应该是禁止从研究环境向外连接数据，无法在研究环境与外部服务器建立连接，只能下载到本地，在从本地环境读取数据并推送到远程服务器。
# 本文件不能直接运行，仅用于米筐的jupyter notebook环境。

import traceback
import socket
from datetime import datetime, timedelta
from typing import Optional, Sequence, List
from rpc.client import RpcClient
from rpc.utility import INTERVAL_ADJUSTMENT_MAP
from rpc.utility import get_duration, extract_vt_symbol, to_rq_symbol, handle_df, load_json, save_json


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
    client.connect()
    try:
        return client
    except:
        traceback.print_exc()


def get_update_symbol(client: RpcClient) -> List:
    symbols = client.get_update_symbol()
    print("待更新的合约列表获取成功：")
    print(symbols)
    return symbols


def save_all_data(client: RpcClient, rq_interval: str, start: datetime, end: datetime, symbols: Optional[Sequence[str]] = None):
    if symbols is None:
        symbols = get_update_symbol(client)
    for vt_symbol in symbols:
        data_dict = query_by_symbol(vt_symbol, rq_interval, start, end)
        client.save_to_database(data_dict, vt_symbol, rq_interval)
        print(f"{vt_symbol}合约数据保存成功")


def save_bars_to_json(symbols: Sequence[str], rq_interval: str, start: datetime, end: datetime):
    all_bars_dict = {}
    for vt_symbol in symbols:
        d = {}
        symbol_bars_list = query_by_symbol(vt_symbol, rq_interval, start, end)
        d["rq_interval"] = rq_interval
        d["data"] = symbol_bars_list

        all_bars_dict[vt_symbol] = d
        print(f"{vt_symbol}装入成功")

    save_json("daily_update_bars.json", all_bars_dict)
    print(f"所有数据存储到json成功")


connect_setting = load_json('connect.json')


# 下载到json
def save_data():
    rq_interval = "60m"
    back_days = 60
    start, end = get_duration(back_days)
    print(start, end)

#     symbols = {'cu2102.SHFE', 'MA105.CZCE', 'ag2106.SHFE', 'p2105.DCE', 'AP105.CZCE', 'sp2103.SHFE', 'bu2106.SHFE', 'RM105.CZCE', 'pp2105.DCE', 'ZC105.CZCE', 'a2105.DCE', 'CF105.CZCE', 'jd2105.DCE', 'ru2105.SHFE', 'SR105.CZCE', 'SM105.CZCE', 'TA105.CZCE', 'rb2105.SHFE'}
    symbols = ['ni2106.SHFE', 'MA106.CZCE']
    save_bars_to_json(symbols, rq_interval, start, end)


save_data()

# 获取服务器连接信息
host_home = socket.gethostbyname(connect_setting['host_home'])
host_tencent = connect_setting['host_tencent']
port = connect_setting['port']
authkey = connect_setting['authkey'].encode('ascii')
print(datetime.now(), host_home)

# 推送到服务器


def update_to_2server():
    rq_interval = "60m"
    back_days = 60
    start, end = get_duration(back_days)
    print(start, end)

    symbols = None
    client_home = init_client(host_home, port, authkey)
    if client_home:
        #         symbols = ['RM005.CZCE']
        save_all_data(client_home, rq_interval, start, end, symbols=symbols)
        client_home.close()

    client_tencent = init_client(host_tencent, port, authkey)
    if client_tencent:
        save_all_data(client_tencent, rq_interval, start, end, symbols=symbols)
        client_tencent.close()

    return client_home, client_tencent


client_h, client_t = update_to_2server()
