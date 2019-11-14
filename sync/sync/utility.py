import os
import json
from pathlib import Path
from datetime import datetime

from .setting import file_setting


def to_dash_format(name: str) -> str:
    """
    把驼峰命名转换为下划线命名
    """
    words = []
    begin_index = 0
    end_index = len(name) - 1
    for index, char in enumerate(name):
        if index > 0:
            if char.isupper():
                last_word = name[begin_index: index].lower()
                words.append(last_word)
                begin_index = index
                if index == end_index:
                    words.append(char.lower())
            else:
                if index == end_index:
                    last_word = name[begin_index:].lower()
                    words.append(last_word)
    return '_'.join(words)


def get_file_path(filename: str) -> Path:
    """
    从文件名获取文件路径Path对象
    """
    # print('file_setting', file_setting)
    # print(filename)
    pre_dir = file_setting.get(filename, None)

    if not pre_dir:
        pre_dir = Path.cwd()
    return Path(pre_dir).joinpath(filename)


def get_file_stat(filename: str) -> os.stat_result:
    """
    获取文件统计信息
    """
    file_path = get_file_path(filename)
    return file_path.stat()


def load_json(filename: str) -> dict:
    """"""
    file_path = get_file_path(filename)
    print(file_path)
    if file_path.exists():
        with open(str(file_path), mode='r', encoding='UTF-8') as f:
            data = json.load(f)
        return data
    else:
        print('no exists')
        return {}


def save_json(filename: str, data: dict):
    """"""
    file_path = get_file_path(filename)
    with open(str(file_path), mode="w+", encoding="UTF-8") as f:
        json.dump(
            data,
            f,
            indent=4,
            ensure_ascii=False
        )


def get_file_modified_time(filename: str) -> datetime:
    """获取本地文件的最新修改时间并仅精确到秒"""
    d = datetime.fromtimestamp(get_file_stat(filename).st_mtime)
    return datetime(d.year, d.month, d.day, d.hour, d.minute, d.second)
