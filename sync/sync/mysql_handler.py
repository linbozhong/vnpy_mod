import traceback
import pymysql
from functools import wraps
from datetime import datetime
from pymysql.cursors import DictCursor
from typing import Callable

from sync.logger import logger


def execute_decorator(func: Callable):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self.cursor_list.append(self.get_cursor())
        # print(self.cursor_list)
        try:
            res = func(self, *args, **kwargs)
            logger.debug(f"{func.__name__} Mysql语句执行成功")
            return res
        except:
            self.db.rollback()
            logger.debug(f"{func.__name__} Mysql语句执行异常")
            traceback.print_exc()
            return 0
        finally:
            self.cursor.close()
            self.cursor_list.pop()
            # print(self.cursor_list)

    return wrapper


class MySqlHandler(object):
    PY_TYPE_TO_MYSQL_FIELD_MAP = {
        int: 'int',
        float: 'double',
        str: 'varchar(255)',
        datetime: 'datetime'
    }

    def __init__(self):
        self.db = None

        # 嵌套执行含有装饰器的函数时，需要多个cursor
        self.cursor_list = []

        self.cursor_type = DictCursor

    def __del__(self):
        if self.db.open:
            self.db.close()
        print('对象析构，断开数据库连接')

    @staticmethod
    def gen_insert_sql(table_name: str, record: dict) -> str:
        """
        """
        ordered_key = list(record.keys())
        keys = [f"`{key}`" for key in ordered_key]
        values = [f"%({key})s" for key in ordered_key]
        keys_str = ', '.join(keys)
        values_str = ', '.join(values)
        sql = f"INSERT INTO `{table_name}` ({keys_str}) VALUES ({values_str});"
        return sql

    @classmethod
    def gen_create_table_sql(cls, table_name: str, field_dict: dict) -> str:
        """
        Parse table field from field dict example.
        field_dict example:
         {
            "pos": 0,
            "fast_ma0": 0.0,
            "slow_ma0": 0.0,
        }
        """
        fields = []
        for key, value in field_dict.items():
            field = f"`{key}` {cls.PY_TYPE_TO_MYSQL_FIELD_MAP[type(value)]} NOT NULL"
            fields.append(field)
        field_sql = ',\n'.join(fields)

        sql = f"""
            CREATE TABLE `{table_name}` (
            {field_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
            """
        return sql

    @staticmethod
    def gen_query_sql(table_name: str, fields: tuple = (), order_field: str = '', order_by: str = 'ASC') -> str:
        if fields:
            fields = [f"`{field}`" for field in fields]
            fields_sql = ','.join(fields)
        else:
            fields_sql = '*'

        if order_field:
            order_by_sql = f"ORDER BY `{order_field}` {order_by}"
        else:
            order_by_sql = ''

        sql = f"SELECT {fields_sql} FROM `{table_name}` {order_by_sql};"
        return sql

    @staticmethod
    def gen_delete_sql(table_name: str, condition: dict) -> str:
        cond_sql = f"`{condition['field']}`{condition['operator']}'{str(condition['value'])}'"
        sql = f"DELETE FROM `{table_name}` WHERE {cond_sql};"
        return sql

    def _execute(self, sql: str, *args, **kwargs):
        self.cursor.execute(sql, *args, **kwargs)
        return self.cursor

    def connect(self, *args, **kwargs):
        try:
            self.db = pymysql.connect(*args, **kwargs)
            logger.info("Mysql连接成功")
        except:
            logger.info('Mysql连接失败')
            traceback.print_exc()

    def set_cursor_type(self, cursor_class: object):
        """
        :param cursor_class: pymysql.cursors.Cursor
        """
        self.cursor_type = cursor_class

    def get_cursor(self):
        return self.db.cursor(self.cursor_type)

    def close_db(self):
        self.db.close()
        logger.info("Mysql连接关闭")

    @property
    def cursor(self):
        return self.cursor_list[-1]

    @execute_decorator
    def get_tables(self, table_name: str, precise: bool = True):
        if not precise:
            table_name = f"%{table_name}%"
        check_table_sql = f"SHOW TABLES LIKE '{table_name}';"
        cursor = self._execute(check_table_sql)
        result = cursor.fetchall()
        return result

    def is_table_exists(self, table_name: str, precise: bool = True):
        res = self.get_tables(table_name, precise)
        return len(res) > 0

    @execute_decorator
    def create_table(self, table_name: str, field_dict: dict):
        if not self.is_table_exists(table_name):
            create_sql = self.gen_create_table_sql(table_name, field_dict)
            self._execute(create_sql)
            logger.debug(f"{table_name}:数据表创建成功")
        else:
            logger.debug(f"{table_name}:数据表已经存在")

    @execute_decorator
    def drop_table(self, table_name: str):
        if self.is_table_exists(table_name):
            drop_sql = f"DROP TABLE `{table_name}`"
            self._execute(drop_sql)
            logger.debug(f"{table_name}:表格表删除成功")
        else:
            logger.debug(f"{table_name}:数据表不存在")

    @execute_decorator
    def insert(self, table_name: str, record: dict):
        sql = self.gen_insert_sql(table_name, record)
        self._execute(sql, record)
        self.db.commit()
        logger.debug("数据插入并提交成功")

    @execute_decorator
    def update(self, table_name: str, new_data: dict, condition: dict):
        # use this sql
        # UPDATE `user` SET `c1` = '2017', `c2` = '名字10' WHERE `id` = 10;
        pass

    @execute_decorator
    def query(self, table_name: str):
        pass

    @execute_decorator
    def delete(self, table_name: str, cond_dict: dict):
        """
        :param table_name:
        :param cond_dict: the key can not be changed.
            example:{
                "field": "user_id"
                "operator": "="
                "value": 10
            }
        :return: str
        """
        sql = self.gen_delete_sql(table_name, cond_dict)
        # print(sql)
        self._execute(sql)
        self.db.commit()
        logger.debug(f"数据表{table_name} 数据删除成功")

    @execute_decorator
    def query_all(self, table_name: str, order_field: str = '', order_by: str = 'ASC'):
        """
        :param table_name:
        :param order_field:
        :param order_by: 'ASC' or 'DESC'
        :return:
        """
        sql = f"SELECT * FROM `{table_name}`"
        if order_field:
            order_by_sql = f"ORDER BY `{order_field}` {order_by}"
            sql = sql + ' ' + order_by_sql
        self._execute(sql)
        res = self.cursor.fetchall()
        logger.debug("所有数据获取成功")
        return res

    @execute_decorator
    def delete_all(self, table_name: str):
        sql = f"DELETE FROM `{table_name}`"
        self._execute(sql)
        self.db.commit()
        logger.debug("所有数据删除成功")
