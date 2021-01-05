import time
from typing import List, Sequence, Optional
from datetime import datetime
from rpc.utility import load_json, extract_vt_symbol, strip_digt, handle_df
from rpc.client import RpcClient
from tqsdk import TqApi, TqAuth


INTERVAL_RQ2TQ = {
    '1m': 60,
    '60m': 3600,
    '1d': 86400
}

bars_dicts = {}


def get_update_symbol(client: RpcClient) -> List:
    symbols = client.get_update_symbol()
    print("待更新的合约列表获取成功：")
    print(symbols)
    return symbols


def vt_symbol_to_tq_symbol(vt_symbol: str, bar_type: str = "trading"):
    """
    bar_type: "trading", "index", "main"
    """
    symbol, exchange = extract_vt_symbol(vt_symbol)
    if bar_type == "trading":
        return f"{exchange.value}.{symbol}"
    elif bar_type == "index":
        return f"KQ.i@{exchange.value}.{strip_digt(symbol)}"
    elif bar_type == "main":
        return f"KQ.m@{exchange.value}.{strip_digt(symbol)}"
    else:
        raise ValueError(
            "The bar_type argument must be trading, index or main")


def init_client(host: str, port: int, authkey: bytes):
    client = RpcClient(host, port, authkey)
    client.connect()
    return client


def query_by_symbol(vt_symbol: str, source_interval: str) -> dict:
    if bars_dicts.get(vt_symbol):
        print(f"{vt_symbol}数据已经获取过，直接从内存读取。")
        return bars_dicts[vt_symbol]
    else:
        tq_symbol = vt_symbol_to_tq_symbol(vt_symbol)
        tq_interval = INTERVAL_RQ2TQ[source_interval]
        df = tq_api.get_kline_serial(tq_symbol, tq_interval)
        df['datetime'] = df['datetime'].map(
            lambda x: datetime.fromtimestamp(x / 1e9))
        df.set_index('datetime', inplace=True, drop=True)
        df = handle_df(df, source_interval, need_adjust=False)
        # print(df)
        bar_dict_list = df.to_dict(orient="records")
        bars_dicts[vt_symbol] = bar_dict_list
        return bar_dict_list


def save_all_data(client: RpcClient, source_interval: str, symbols: Optional[Sequence[str]] = None):
    if symbols is None:
        symbols = get_update_symbol(client)
    for vt_symbol in symbols:
        data_dict = query_by_symbol(vt_symbol, source_interval)
        client.save_to_database(data_dict, vt_symbol, source_interval)
        print(f"{vt_symbol}合约数据保存成功")


if __name__ == "__main__":
    setting = load_json('setting.json')
    print(setting)

    host_home = '192.168.0.107' if setting['is_at_home'] else setting['host_home']
    host_tencent = setting['host_cloud']
    port = setting['port']
    authkey = setting['authkey'].encode('ascii')
    source_interval = "60m"

    # print("连接天勤数据，等待10s..")
    auth = TqAuth(setting['tqsdk_user'], setting['tqsdk_pw'])
    tq_api = TqApi(auth=auth)
    # time.sleep(5)
    # print("连接等待时间结束")


    client_home = init_client(host_home, port, authkey)
    if client_home:
        symbols = get_update_symbol(client_home)
        symbols = ['ni2105.SHFE', 'zn2105.SHFE']
        save_all_data(client_home, source_interval, symbols)
        client_home.close()

    client_tencent = init_client(host_tencent, port, authkey)
    if client_tencent:
        symbols = get_update_symbol(client_tencent)
        symbols = ['ni2105.SHFE', 'zn2105.SHFE']
        save_all_data(client_tencent, source_interval, symbols)
        client_tencent.close()


    # test
    # query_by_symbol('rb2105.SHFE', source_interval)
    # query_by_symbol('rb2105.SHFE', source_interval)
    # rb = tq_api.get_kline_serial('SHFE.rb2105', 3600)
    # cu = tq_api.get_kline_serial('SHFE.cu2105', 3600)

    print("执行完成，进程阻塞中..")
    while True:
        pass
        # print(rb)
        # print(cu)
        # tq_api.wait_update()
