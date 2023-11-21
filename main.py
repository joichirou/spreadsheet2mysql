import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime as dt
import pandas as pd
from pprint import pprint
import sys
import os
import json
import db
import myutil

# コンフィグ設定
CONFIG = {}
with open(os.path.dirname(__file__) + '/config.json', encoding="utf-8") as f:
    CONFIG = json.load(f)

class SpreadSheet():

    def __init__(self, db_host=CONFIG['DB_HOST'], debug_mode=CONFIG['DEBUG_MODE'], insert_mode=CONFIG['INSERT_MODE']):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.path.dirname(__file__)+'/auth_key_knishi.json', scope)
        gs = gspread.authorize(creds)
        self.debug_mode = False
        self.sheet_key = CONFIG['spreadsheet']['id']
        self.workbook = gs.open_by_key(self.sheet_key)
        self.db_host = db_host
        self.insert_mode = insert_mode
        self.debug_mode = debug_mode
        self.table = CONFIG['table']
        self.log_sign = '[MAIN]'
        self.log_file = CONFIG['LOG_FILE']

    def create_table(self, db_name):
        sql_base = "CREATE TABLE IF NOT EXISTS `{}` ({}) ENGINE={};"
        col_list = []
        for col in self.table['columns']:
            col_list.append(
                "`{}` {} {} {}{}".format(
                    col['name'],
                    col['type'],
                    'NULL' if col['null'] else 'NOT NULL',
                    'DEFAULT ' + col['default'] if col['default'] else '',
                    col['option'] if 'option' in col.keys() else ''
                )
            )
        sql_part_col = ",\n".join(col_list)
        if self.table['primary_key']:
            sql_part_col = sql_part_col + ", PRIMARY KEY (`{}`)".format(self.table['primary_key'])
        sql = sql_base.format(
            self.table['name'],
            sql_part_col,
            self.table['engine'])
        self.output_log('SQL: ' + sql)
        db_config={}
        db_config['host'] = 'localhost'
        db_config['port'] = 3366
        db_config['db'] = db_name
        db_config['user'] = 'root'
        db_config['pass'] = 'root'
        analytics_db = db.AnalyticsDB(config=db_config)
        #analytics_db.db_host = self.db_host
        res = analytics_db.do_sql(sql)
        self.output_log("Result(create table): " + str(res))

    def output_log(self, msg):
        log_msg = self.log_sign + ' ' + msg
        if self.debug_mode:
            print(log_msg)
        myutil.output_log(self.log_file, log_msg)

    def get_data(self):
        worksheet = self.workbook.worksheet(CONFIG['spreadsheet']['name'])
        all_rec = worksheet.get_all_values()
        rec_list = []
        for row_idx,row in enumerate(all_rec):
            # ヘッダー行はスキップ
            if row_idx == 0:
                continue
            dict = {}
            for info in CONFIG['spreadsheet']['pair']:
                #print(info)
                #print(row)
                dict[info['col']] = row[int(info['idx'])]
            rec_list.append(dict)
        pprint(rec_list)
        return rec_list
    
    def insert_data(self, data, db_name):
        self.create_table(db_name)
        # Table Format(Column)
        col_list = [info['col'] for info in CONFIG['spreadsheet']['pair']]
        now = dt.today()
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")
        insert_list = []
        for d in data:
            list = []
            for col in col_list:
                list.append(d[col])
            insert_list.append(list)
        # レコード作成日時設定
        col_list.append("created")
        for insert in insert_list:
            insert.append(str_now)
        pprint(insert_list)
        self.output_log('insert_list:' + str(len(insert_list)))
        # INSERT
        if self.insert_mode:
            db_config={}
            db_config['host'] = 'localhost'
            db_config['port'] = 3366
            db_config['db'] = db_name
            db_config['user'] = 'root'
            db_config['pass'] = 'root'
            analytics_db = db.AnalyticsDB(config=db_config)
            #analytics_db.db_host = self.db_host
            analytics_db.do_sql("TRUNCATE TABLE " + analytics_db.with_scheme(self.table['name'])) # Rset Table
            result = analytics_db.insert_many_iferr_switch_insert(self.table['name'], col_list, insert_list)
            self.output_log('Insert Results:' + str(result))
        else:
            self.output_log('Skipped insert data.')


def main(host, insert_mode, debug_mode, db_name):
    # ログファイル初期化
    myutil.clear_log_file(os.path.dirname(__file__) + '/' + CONFIG['LOG_FILE'])
    sheet = SpreadSheet(host, insert_mode, debug_mode)
    data = sheet.get_data()
    sheet.insert_data(data, db_name)


if __name__ == '__main__':
    import argparse
    import myutil
    # コマンドライン引数設定
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", help="optional. デバッグメッセージを出力.", action="store_true")
    parser.add_argument("--insert", help="optional. インサート処理を行うか.", action="store_true")
    args = parser.parse_args()
    # コンフィグ読み込み
    host = CONFIG['DB_HOST']
    debug_mode = CONFIG['DEBUG_MODE']
    insert_mode = CONFIG['INSERT_MODE']
    db_name = CONFIG['DB_NAME']
    # プログラムオプション設定
    if args.debug:
        debug_mode = True
    if args.insert:
        insert_mode = True
    print("Debug-mode: " + str(debug_mode))
    print("Insert-mode: " + str(insert_mode))
    print("DB-Name: " + str(db_name))
    main(host, debug_mode, insert_mode, db_name)
