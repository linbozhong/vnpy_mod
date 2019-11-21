import traceback
from time import sleep

from sync.logger import logger
from sync.setting import mysql_setting, Content, MYSQL_SETTING_FILENAME
from sync.mysql_handler import MySqlHandler
from sync.sync_script import init, sync
from sync.utility import load_json

mysql_setting.update(load_json(MYSQL_SETTING_FILENAME))
print(mysql_setting)


def main():
    logger.info(f"连接数据库 {mysql_setting['host']}:{mysql_setting['port']}")
    mysql = MySqlHandler()
    mysql.connect(**mysql_setting)

    logger.info("数据库连接等待...")
    for i in reversed(range(5)):
        logger.info(f"等待倒计时:{i}")
        sleep(1)

    try:
        init()
        # for content in [Content.CTA_SETTING, Content.CTA_DATA, Content.FOLLOW_DATA]:
        #     sync(mysql, content)
        sync(mysql, Content.FOLLOW_DATA)
    except:
        traceback.print_exc()
        mysql.close_db()


if __name__ == '__main__':
    main()
