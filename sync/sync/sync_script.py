# coding: utf-8

from pymysql.cursors import Cursor, DictCursor
from datetime import datetime
from copy import copy
from collections import defaultdict
from typing import Optional, List, Tuple, Dict

from sync.logger import logger
from sync.mysql_handler import MySqlHandler
from sync.utility import (to_dash_format, get_file_path, get_file_modified_time, load_json, save_json)
from sync.setting import (CTA_DATA_FILENAME, CTA_SETTING_FILENAME, FOLLOW_DATA_FILENAME, Content)

# global variables
cta_settings = {}
cta_data = {}
follow_data = {}
content_map = {}
strategy_to_class_name_map = {}


def get_strategy_to_class_name() -> dict:
    """
    从cta配置和cta数据文件中建立 策略名->策略类名（下划线式） 的映射字典
    结果示例:
    {
        "rb_atrrsi": "atr_rsi_strategy"
    }
    """
    map_ = dict()
    for strategy_name in cta_data:
        class_name = cta_settings[strategy_name]['class_name']
        map_[strategy_name] = to_dash_format(class_name)
    return map_


def init() -> None:
    """
    初始化全局变量
    """
    cta_data.update(load_json(CTA_DATA_FILENAME))
    cta_settings.update(load_json(CTA_SETTING_FILENAME))
    follow_data.update(load_json(FOLLOW_DATA_FILENAME))
    strategy_to_class_name_map.update(get_strategy_to_class_name())

    temp_map = {
        Content.CTA_SETTING: {
            "filename": CTA_SETTING_FILENAME,
            "create_func": get_cta_setting_fields,
            "to_server_func": cta_setting_to_server,
            "from_server_func": cta_setting_from_server
        },
        Content.CTA_DATA: {
            "filename": CTA_DATA_FILENAME,
            "create_func": get_cta_data_fields,
            "to_server_func": cta_data_to_server,
            "from_server_func": cta_data_from_server
        },
        Content.FOLLOW_DATA: {
            "filename": FOLLOW_DATA_FILENAME,
            "create_func": get_follow_data_fields,
            "to_server_func": follow_data_to_server,
            "from_server_func": follow_data_from_server
        }
    }
    content_map.update(temp_map)
    logger.info("数据初始化完成")


def strategy_to_table_name(strategy_name: str, content: Content) -> str:
    """
    cta策略名转换为mysql表格名
    结果示例:
    {
        "rb_atrrsi": "cta_data_atr_rsi"
    }
    """
    table_name = strategy_to_class_name_map[strategy_name].replace('_strategy', '')
    return f"{content.value}_{table_name}"


def get_cond_dict(strategy_name: str) -> dict:
    """
    从策略名获取自定义的mysql handler条件过滤字典
    """
    cond_dict = {
        "field": "strategy_name",
        "operator": "=",
        "value": strategy_name
    }
    return cond_dict


def get_cta_data_fields(extend_field: Optional[dict] = None) -> List[Tuple[str, Dict]]:
    """
    返回cta策略数据示例，以自动生成数据库字段
    example:
    [
        ('cta_data_atr_rsi', {'pos': 0, 'atr_value': 0, 'rsi_value': 0}),
        ('cta_data_double_ma', {'pos': 0, 'fast_ma0': 0.0, 'slow_ma0': 0.0})
        ...
    ]
    """
    added_class_name = []
    res_list = []
    for strategy_name, setting in cta_settings.items():
        table_name = strategy_to_table_name(strategy_name, Content.CTA_DATA)
        if table_name not in added_class_name:
            field_dict = copy(cta_data[strategy_name])
            field_dict['strategy_name'] = ''
            field_dict['last_modified_time'] = datetime.now()
            if extend_field:
                field_dict.update(extend_field)
            row = (table_name, field_dict)
            res_list.append(row)
            added_class_name.append(table_name)
    return res_list


def get_cta_setting_fields(extend_field: Optional[dict] = None) -> List[Tuple[str, Dict]]:
    """
    返回cta策略参数示例，以自动生成数据库字段
    example:
    [
        ('cta_setting_atr_rsi', {'pos': 0, 'atr_value': 0, 'rsi_value': 0}),
        ('cta_setting_double_ma', {'pos': 0, 'fast_ma0': 0.0, 'slow_ma0': 0.0})
        ...
    ]
    """
    added_class_name = []
    res_list = []
    for strategy_name, setting in cta_settings.items():
        table_name = strategy_to_table_name(strategy_name, Content.CTA_SETTING)
        if table_name not in added_class_name:
            field_dict = copy(cta_settings[strategy_name]['setting'])
            field_dict['vt_symbol'] = cta_settings[strategy_name]['vt_symbol']
            field_dict['strategy_name'] = ''
            field_dict['class_name'] = ''
            field_dict['last_modified_time'] = datetime.now()
            if extend_field:
                field_dict.update(extend_field)

            row = (table_name, field_dict)
            res_list.append(row)
            added_class_name.append(table_name)
    return res_list


def get_follow_data_fields() -> List[Tuple[str, Dict]]:
    """
    返回跟随交易数据示例，以自动生成数据库字段
    example:
    [
        ('follow_data_trade_ids', {'follow_id': '', .... }),
        ('double_ma_setting', {'source_long': 0, 'source_short': 0, ....})
    ]
    """
    ids_dict = {
        'follow_id': '',
        'order_id': '',
        'last_modified_time': datetime.now()
    }
    trade_ids = ('follow_data_trade_ids', ids_dict)

    pos_dict = {
        "source_long": 0,
        "source_short": 0,
        "target_long": 0,
        "target_short": 0,
        "vt_symbol": "",
        "last_modified_time": datetime.now()
    }
    positions = ("follow_data_positions", pos_dict)
    return [trade_ids, positions]


def create_table(mysql_handler: MySqlHandler, content: Content) -> None:
    """
    通过前缀名创建数据库表格
    content: 'setting' or 'data' or 'follow_data'
    """
    create_func = content_map[content]['create_func']
    fields_list = create_func()
    for (table_name, field_dict) in fields_list:
        mysql_handler.create_table(table_name, field_dict)


def get_table_list(mysql_handler: MySqlHandler, content: Content) -> List:
    """
    通过前缀名，从数据库获取对应的数据表名称列表
    """
    mysql_handler.set_cursor_type(Cursor)
    tables = mysql_handler.get_tables(content.value, False)
    tables = [item[0] for item in tables]
    mysql_handler.set_cursor_type(DictCursor)
    return tables


def get_local_modified_time(content: Content) -> datetime:
    """获取本地数据最新修改时间"""
    local_file_name = content_map[content]['filename']
    return get_file_modified_time(local_file_name)


def get_server_modified_time(mysql_handler: MySqlHandler, content: Content) -> datetime:
    """获取数据库数据最新修改时间"""
    tables = get_table_list(mysql_handler, content)
    table_name = tables[0]

    server_record = mysql_handler.query_all(table_name, order_field='last_modified_time', order_by='DESC')
    server_time = server_record[0]['last_modified_time']
    return server_time


def is_local_exist(content: Content) -> bool:
    """
    本地数据是否存在
    """
    try:
        filename = content_map[content]['filename']
    except KeyError:
        return False
    return get_file_path(filename).exists()


def is_remote_exist(mysql_handler: MySqlHandler, content: Content) -> bool:
    """
    远程数据是否存在
    """
    return mysql_handler.is_table_exists(content.value, precise=False)


def cta_setting_to_server(mysql_handler: MySqlHandler) -> None:
    """
    推送本地cta配置到数据库
    """
    modified_time = get_file_modified_time(CTA_SETTING_FILENAME)
    for strategy_name, settings in cta_settings.items():
        row = dict()
        row['strategy_name'] = strategy_name
        row['class_name'] = settings['class_name']
        row['vt_symbol'] = settings['vt_symbol']
        row['last_modified_time'] = modified_time
        row.update(settings['setting'])

        table_name = strategy_to_table_name(strategy_name, Content.CTA_SETTING)
        # print(table_name)

        # delete first.
        mysql_handler.delete(table_name, get_cond_dict(strategy_name))
        # insert
        mysql_handler.insert(table_name, row)
        logger.info(f"策略{strategy_name}：配置同步到远程成功")


def cta_setting_from_server(mysql_handler: MySqlHandler) -> dict:
    """
    从数据库获取cta配置
    """
    tables = get_table_list(mysql_handler, Content.CTA_SETTING)
    server_settings = dict()
    for table in tables:
        table_res = mysql_handler.query_all(table)
        # print(table_res)
        for row_dict in table_res:
            strategy_name = row_dict['strategy_name']
            server_settings[strategy_name] = dict()
            temp = server_settings[strategy_name]
            temp['class_name'] = row_dict['class_name']
            temp['vt_symbol'] = row_dict['vt_symbol']

            for key in ['vt_symbol', 'strategy_name', 'last_modified_time']:
                row_dict.pop(key)
            temp['setting'] = row_dict
    # print(server_settings)
    return server_settings


def cta_data_to_server(mysql_handler: MySqlHandler) -> None:
    """
    推送本地cta数据到数据库
    """
    modified_time = get_file_modified_time(CTA_DATA_FILENAME)
    for strategy_name, data in cta_data.items():
        row = dict()
        row['strategy_name'] = strategy_name
        row['last_modified_time'] = modified_time
        row.update(data)

        table_name = strategy_to_table_name(strategy_name, Content.CTA_DATA)

        # delete first.
        mysql_handler.delete(table_name, get_cond_dict(strategy_name))
        # insert
        mysql_handler.insert(table_name, row)
        logger.info(f"策略{strategy_name}：数据同步到远程成功")


def cta_data_from_server(mysql_handler: MySqlHandler) -> dict:
    """
    从数据库获取cta数据
    """
    tables = get_table_list(mysql_handler, Content.CTA_DATA)
    server_data = dict()
    for table in tables:
        table_res = mysql_handler.query_all(table)
        # print(table_res)
        for row_dict in table_res:
            strategy_name = row_dict['strategy_name']
            server_data[strategy_name] = dict()
            for key in ['strategy_name', 'last_modified_time']:
                row_dict.pop(key)
            server_data[strategy_name].update(row_dict)

    # print(server_data)
    return server_data


def follow_data_from_server(mysql_handler: MySqlHandler) -> dict:
    """
    从数据库获取跟随数据
    """
    server_data = dict()

    # sync ids
    ids_list = mysql_handler.query_all('follow_data_trade_ids')
    # print(ids_list)
    ids_dict = defaultdict(list)
    for row_dict in ids_list:
        follow_id = row_dict['follow_id']
        order_id = row_dict['order_id']
        ids_dict[follow_id].append(order_id)
    # print(ids_dict)

    # sync pos
    pos_list = mysql_handler.query_all('follow_data_positions')
    # print(pos_list)
    pos_dict = dict()
    for row_dict in pos_list:
        vt_symbol = row_dict['vt_symbol']
        pos_dict[vt_symbol] = dict()

        for key in ['vt_symbol', 'last_modified_time']:
            row_dict.pop(key)
        pos_dict[vt_symbol].update(row_dict)
    # print(pos_dict)

    server_data['tradeid_orderids_dict'] = ids_dict
    server_data['positions'] = pos_dict
    # print(server_data)
    return server_data


def follow_data_to_server(mysql_handler: MySqlHandler) -> None:
    """
    推送跟随数据到数据库
    """
    modified_time = get_file_modified_time(FOLLOW_DATA_FILENAME)

    # sync trade ids
    id_table_name = 'follow_data_trade_ids'
    mysql_handler.delete_all(id_table_name)
    trade_ids = follow_data['tradeid_orderids_dict']
    for trade_id, order_list in trade_ids.items():
        row = dict()
        row['follow_id'] = trade_id
        row['last_modified_time'] = modified_time
        for order in order_list:
            row['order_id'] = order
            mysql_handler.insert(id_table_name, row)
    logger.info(f"跟随单ID：数据同步到远程成功")

    # sync positions
    pos_table_name = 'follow_data_positions'
    mysql_handler.delete_all(pos_table_name)
    positions = follow_data['positions']
    for vt_symbol, pos_dict in positions.items():
        row = dict()
        row['vt_symbol'] = vt_symbol
        row['last_modified_time'] = modified_time
        row.update(pos_dict)

        # insert
        mysql_handler.insert(pos_table_name, row)
    logger.info(f"跟随单仓位：数据同步到远程成功")


def sync(mysql_handler: MySqlHandler, content: Content) -> None:
    """
    自动同步
    """
    logger.info(f"正在同步数据：{content.value}")
    filename = content_map[content]['filename']
    sync_to_server_func = content_map[content]['to_server_func']
    sync_from_server_func = content_map[content]['from_server_func']

    local_exist = False
    remote_exist = False
    local_time = None
    server_time = None

    if is_local_exist(content):
        local_exist = True
        local_time = get_local_modified_time(content)

    if is_remote_exist(mysql_handler, content):
        remote_exist = True
        server_time = get_server_modified_time(mysql_handler, content)

    if local_exist:
        logger.info("本地数据已存在")
        if remote_exist:
            logger.info("远程数据已存在")
            if local_time > server_time:
                logger.info("本地数据是最新的")
                sync_to_server_func(mysql_handler)
                logger.info("上传数据成功")
            elif server_time > local_time:
                logger.info("本地数据不是最新的")
                data = sync_from_server_func(mysql_handler)
                save_json(filename, data)
                logger.info("数据获取成功")
            else:
                logger.info("本地数据与数据库时间戳一致，无需同步")
        else:
            logger.info("远程数据不存在")
            create_table(mysql_handler, content)
            logger.info("数据表创建成功")
            sync_to_server_func(mysql_handler)
            logger.info("上传数据成功")
    else:
        logger.info("本地数据不存在")
        if remote_exist:
            logger.info("远程数据已存在")
            data = sync_from_server_func(mysql_handler)
            save_json(filename, data)
            logger.info("数据获取成功")
        else:
            logger.info("本地和远程数据不存在")
