import unittest
from time import sleep
from sync.utility import *
from sync.mysql_handler import MySqlHandler
from sync.setting import (mysql_setting, CTA_SETTING_FILENAME, CTA_DATA_FILENAME, Content, MYSQL_SETTING_FILENAME)
from sync.sync_script import (init, sync,
                              get_strategy_to_class_name, strategy_to_table_name,
                              strategy_to_class_name_map, content_map,
                              get_cta_data_fields, get_cta_setting_fields, get_follow_data_fields,
                              create_table, get_table_list, clear_server_data,
                              is_local_exist, is_remote_exist, get_local_modified_time, get_server_modified_time,
                              cta_setting_to_server, cta_data_to_server, follow_data_to_server,
                              cta_setting_from_server, cta_data_from_server, follow_data_from_server)

mysql_setting.update(load_json(MYSQL_SETTING_FILENAME))

mysql_setting['host'] = "192.168.0.107"
print(mysql_setting)

init()
mysql = MySqlHandler()
mysql.connect(**mysql_setting)

print("数据库连接等待时间...")
sleep(3)
print("数据库连接等待结束")


class TestUtility(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def test_to_dash_format(self):
        print(to_dash_format('_dfHeoo'))
        print(to_dash_format('NormalWorld'))
        print(to_dash_format('HHello'))
        print(to_dash_format('_dfHeooHHH'))

    def test_get_file_path(self):
        print(get_file_path(CTA_SETTING_FILENAME))
        print(get_file_path(CTA_DATA_FILENAME))
        print(get_file_path('test.json'))
        print(get_file_stat(CTA_DATA_FILENAME))

    def test_json(self):
        print(load_json(CTA_DATA_FILENAME))
        print(load_json('test.json'))

        test_d = {'a': 1, 'b': 2}
        save_json('test_d.json', test_d)

    def test_get_modified_time(self):
        print(get_file_modified_time(CTA_DATA_FILENAME))


class TestSyncScript(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def test_view_vars(self):
        print(strategy_to_class_name_map)
        print(content_map)

    def test_convert_strategy_name(self):
        print(get_strategy_to_class_name())
        print(strategy_to_table_name('rb_atrrsi', Content.CTA_DATA))
        print(strategy_to_table_name('rb_atrrsi', Content.CTA_SETTING))

    def test_get_fields(self):
        print(get_cta_data_fields())
        print(get_cta_setting_fields())
        print(get_follow_data_fields())

    def test_create_table(self):
        create_table(mysql, Content.CTA_DATA)
        create_table(mysql, Content.CTA_SETTING)
        create_table(mysql, Content.FOLLOW_DATA)

    def test_get_tables(self):
        print(get_table_list(mysql, Content.CTA_DATA))
        print(get_table_list(mysql, Content.CTA_SETTING))
        print(get_table_list(mysql, Content.FOLLOW_DATA))

    def test_exist(self):
        print(Content.CTA_DATA)
        print('local exists', is_local_exist(Content.CTA_DATA))
        # print('local non-exist', is_local_exist('non_content'))
        print('remote exist', is_remote_exist(mysql, Content.CTA_DATA))
        # print('remote non-exist', is_remote_exist(mysql, 'follow_setting'))

    def test_clear_server_table(self):
        clear_server_data(mysql, Content.CTA_SETTING)
        clear_server_data(mysql, Content.CTA_DATA)
        clear_server_data(mysql, Content.FOLLOW_DATA)

    def test_cta_setting_to_server(self):
        cta_setting_to_server(mysql)

    def test_cta_data_to_server(self):
        cta_data_to_server(mysql)

    def test_follow_data_to_server(self):
        follow_data_to_server(mysql)

    def test_modified_time(self):
        print(get_local_modified_time(Content.CTA_DATA))
        print(get_server_modified_time(mysql, Content.CTA_DATA))

    def test_from_server(self):
        server_settings = cta_setting_from_server(mysql)
        save_json('cta_setting_test.json', server_settings)

        server_data = cta_data_from_server(mysql)
        save_json('cta_data_test.json', server_data)

        follow_data = follow_data_from_server(mysql)
        save_json('follow_data_test.json', follow_data)

    def test_cta_setting_sync(self):
        sync(mysql, Content.CTA_SETTING)

    def test_cta_data_sync(self):
        sync(mysql, Content.CTA_DATA)

    def test_follow_data_sync(self):
        sync(mysql, Content.FOLLOW_DATA)


class TestMysql(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def test_check_table(self):
        print(mysql.is_table_exists('hour_turtle_signal_strategy'))
        print(mysql.is_table_exists('no_exist'))
        print(mysql.is_table_exists('rsi', precise=False))
        print(mysql.is_table_exists('rsi', precise=True))
        print("Not precise:", mysql.is_table_exists('data', precise=False))
        print("Precise:", mysql.is_table_exists('data', precise=True))

    def test_get_sql_tables(self):
        # 有数据
        print(mysql.get_tables('data', precise=False))

        # 无数据
        print(mysql.get_tables('atr_rsi_data', precise=True))
        print(mysql.get_tables('helloworld', precise=False))
        tables = mysql.get_tables('helloworld', precise=True)
        print(tables, type(tables))

    def test_delete(self):
        print(mysql.gen_delete_sql('test', {'field': 'user', 'operator': '=', 'value': 'lin'}))
        print(mysql.gen_delete_sql('atr_rsi_data', {'field': 'rsi_sell', 'operator': '=', 'value': 34}))
        # mysql.delete('atr_rsi_data', {'field': 'rsi_sell', 'operator': '=', 'value': 34})
        # mysql.delete_all('double_ma_strategy')

    def test_drop_table(self):
        mysql.drop_table("cta_data_atr_rsi")
        mysql.drop_table("cta_setting_atr_rsi")

    def test_insert(self):
        pass
        # "2019-11-08 13:01:01"
        # a_double_ma = {
        #     "pos": 0,
        #     "fast_ma0": 0.0,
        #     "fast_ma1": 0.0,
        #     "slow_ma0": 0.0,
        #     "slow_ma1": 0.0,
        #     "strategy_name": "a_double_ma",
        #     "last_modified_time": datetime.now()
        # }
        # mysql.insert('double_ma_strategy', a_double_ma)

    def test_query(self):
        pass
        # # test query all
        # print(mysql.query_all('double_ma_strategy'))
        # # order by asc
        # print(mysql.query_all('double_ma_strategy', order_field='last_modified_time'))
        # # order by desc
        # print(mysql.query_all('double_ma_strategy', order_field='last_modified_time', order_by='DESC'))


if __name__ == '__main__':
    unittest.main()
