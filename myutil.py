"""
===============================================================================>

汎用メソッド

===============================================================================>
"""
from datetime import datetime as dt
from datetime import date, timedelta
from dateutil import tz
import json
import yaml
import os

# ログファイル初期化
def clear_log_file(log_file):
    with open(log_file, 'w') as f:
        f.write('')

# ログ出力
def output_log(log_file, msg, log_sign='[+]', debug_mode=False):
    if isinstance(msg, list) or isinstance(msg, dict):
        if debug_mode:
            print(msg)
        if log_file:
            with open(log_file, mode='a') as f:
                f.write(log_date() + ' ' + log_sign + ' ' + json.dumps(msg) + '\n')
    else:
        if debug_mode:
            print(log_sign + ' ' + msg)
        if log_file:
            with open(log_file, mode='a') as f:
                f.write(log_date() + ' ' + log_sign + ' ' + msg + '\n')

# ログ日時フォーマット
def log_date():
    now = dt.today()
    return '[' + now.strftime("%Y-%m-%d %H:%M:%S") + ']'

# コンフィグファイルのロード
def load_config(config_file):
    with open(config_file, encoding="utf-8") as f:
        return yaml.safe_load(f)

def str_to_date(str_date):
        return dt.strptime(str_date, '%Y-%m-%d')

def date_to_str(date):
    return date.strftime('%Y-%m-%d')

def slice_list(list, n):
    res = []
    for i in range(0, len(list), n):
        res.append(list[i: i+n])
    return res

def file_list_on_dir(path: str):
    files = os.listdir(path)
    return [f for f in files if os.path.isfile(os.path.join(path, f))]

def dir_list_on_dir(path: str):
    files = os.listdir(path)
    return [f for f in files if os.path.isdir(os.path.join(path, f))]

# UTC⇒JST変換
def cnv_utc_to_jst(utc) -> str:
    utc = utc.replace('Z', '+00:00')
    dt_utc = dt.fromisoformat(utc)
    JST = tz.gettz('Asia/Tokyo')
    dt_jst = dt_utc.astimezone(JST)
    return dt_jst.strftime('%Y-%m-%d %H:%M:%S')

def get_insert_col_list(table_struct):
    col_list = []
    for col in table_struct['columns']:
        if col['name'] not in table_struct['ignore']:
            col_list.append(col['name'])
    return col_list

def get_insert_data(col_list, data):
    value = []
    for col in col_list:
        v = data[col]
        value.append(v)
    return value

def get_insert_data_with_c(col_list, data):
    value = []
    for col in col_list:
        key = col[:-2] # 末尾2文字の_cを除去
        v = data[key]
        value.append(v)
    return value

def get_insert_data_list(col_list, data_list):
    if not data_list:
        print('list data is empty.')
        print(data_list)
        return []
    insert_list = []
    for data in data_list:
        insert_list.append(get_insert_data(col_list, data))
    return insert_list

def list2dict_by_key(list, key) -> dict:
    u"""
    listデータを指定key毎のリストに入れ替える
    """
    res = {}
    for row in list:
        if row[key] not in res.keys():
            res[row[key]] = []
        res[row[key]].append(row)
    return res

def get_sunday_of_the_week(date):
    u"""
    その週の日曜日を取得
    """
    # 日曜を起点にどれだけ離れているかの定義
    week = [-1, -2, -3, -4, -5, -6, 0]
    dt_date = dt.strptime(date, '%Y-%m-%d')
    weekday = dt_date.weekday()
    reduce_days = week[weekday]
    sunday = dt_date + timedelta(reduce_days)
    return sunday.strftime('%Y-%m-%d')

def get_last_sunday_before(date):
    u"""
    1つ前の週の日曜日を取得
    """
    sunday = get_sunday_of_the_week(date)
    dt_sunday = dt.strptime(sunday, '%Y-%m-%d')
    dt_sunday += timedelta(-7)
    return dt_sunday.strftime('%Y-%m-%d')
