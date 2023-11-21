"""
===============================================================================>

DB接続ライブラリ

===============================================================================>
"""
import pymysql.cursors
import pandas as pd
import sqlalchemy as sa
from pprint import pprint
import traceback
import sys
import sqlite3


def get_db_obj(db_type=None, host='localhost', config=None):
    if config:
        return MariaDB(config=config)
    if db_type == 'core':
        return CoreDB(db_host=host)
    if db_type == 'online':
        return OpDB(db_host=host)
    if db_type == 'analytics':
        return AnalyticsDB(db_host=host)
    return None

def get_sql_for_create_table(table):
    sql_base = "CREATE TABLE IF NOT EXISTS `{}` ({}) COMMENT='{}' ENGINE={};"
    col_list = []
    for col in table['columns']:
        col_list.append(
            "`{}` {} {} {}{}{}".format(
                col['name'],
                col['type'],
                'NULL' if col['allow_null'] else 'NOT NULL',
                'DEFAULT ' + col['default'] if col['default'] else '',
                col['option'] if 'option' in col.keys() else '',
                ' COMMENT "{}"'.format(col['comment']) if 'comment' in col.keys() else ''
            )
        )
    sql_part_col = ",\n".join(col_list)
    # 主キー設定
    if table['primary_key']:
        sql_part_col = sql_part_col + ", PRIMARY KEY (`{}`)".format(table['primary_key'])
    # インデックスキー設定
    if 'index' in table.keys():
        index_str_list = []
        for index_col in table['index']:
            index_str_list.append('INDEX `{}` (`{}`)'.format(index_col, index_col))
        sql_part_col = sql_part_col + ", " + ','.join(index_str_list)
    sql = sql_base.format(
        table['name'],
        sql_part_col,
        table['comment'],
        table['engine'])
    return sql

"""
MariaDB操作用クラス
"""
class MariaDB:

    def __init__(self, config=None, db_host='localhost'):
        self.connection = None
        self.engine = None
        self.db_host = db_host
        self.db_port = None
        self.db_name = None
        self.db_user = None
        self.db_pass = None
        self.load_config(config)

    def load_config(self, config):
        if not config:
            return
        #print('Over write database config.')
        #print(config)
        self.db_host = config['host']
        self.db_port = config['port']
        self.db_name = config['db']
        self.db_user = config['user']
        self.db_pass = config['pass']

    def connect(self):
        self.connection = pymysql.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_pass,
            database=self.db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
    
    def create_engine(self):
        url = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(
            self.db_user,
            self.db_pass,
            self.db_host,
            self.db_port,
            self.db_name
        )
        self.engine = sa.create_engine(url, echo=False)

    def create_table(self, table):
        res = None
        try:
            sql = get_sql_for_create_table(table)
            res = self.do_sql(sql)
            print('create table: ' + table['name'])
        except Exception as e:
            t, v, tb = sys.exc_info()
            pprint(traceback.format_exception(t,v,tb))
            pprint(traceback.format_tb(e.__traceback__))
            print('CREATE TABLE SQL: ' + sql)
        return res

    def drop_table(self, table):
        drop_sql = "DROP TABLE IF EXISTS {}".format(table)
        self.do_sql(drop_sql)
        print(drop_sql)

    def truncate_table(self, table_name: str):
        sql = "TRUNCATE TABLE " + self.with_scheme(table_name)
        self.do_sql(sql)
        print(sql)

    def fetch_one(self, sql):
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            return result

    def fetch_all(self, sql):
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
    
    def fetch_all_df(self, sql):
        self.create_engine()
        return pd.read_sql(sql, con=self.engine)
    
    def do_sql(self, sql):
        self.connect()
        with self.connection.cursor() as cursor:
            result = cursor.execute(sql)
            if result > 0:
                self.connection.commit()
        return result

    def insert(self, table, col_list, value):
        self.connect()
        with self.connection.cursor() as cursor:
            placeholder_list = ["%s" for i in range(len(col_list))]
            sql = "INSERT INTO " + self.with_scheme(table) + " (" + ', '.join(col_list) + ") values (" + ', '.join(placeholder_list) + ")"
            result = cursor.execute(sql, value)
            if result > 0:
                self.connection.commit()
                result = cursor.lastrowid
            return result

    def insert_many(self, table, col_list, value_list):
        self.connect()
        with self.connection.cursor() as cursor:
            placeholder_list = ["%s" for i in range(len(col_list))]
            sql = "INSERT INTO " + self.with_scheme(table) + " (" + ', '.join(col_list) + ") values (" + ', '.join(placeholder_list) + ")"
            print(sql)
            print(value_list)
            result = cursor.executemany(sql, value_list)
            if result > 0:
                self.connection.commit()
            return result

    # バルクインサート。もしエラーになればインサート処理に切り替える
    def insert_many_iferr_switch_insert(self, table, col_list, value_list):
        msg = ''
        try:
            result = self.insert_many(table, col_list, value_list)
            msg = 'BULK INSERT Results:{}'.format(str(result))
        except Exception as e:
            print('switch insert one mode')
            result_list = []
            try:
                for insert_row in value_list:
                    result_list.append(self.insert(table, col_list, insert_row))
            except Exception as e:
                t, v, tb = sys.exc_info()
                pprint(traceback.format_exception(t,v,tb))
                pprint(traceback.format_tb(e.__traceback__))
                print(col_list)
                print(insert_row)
            msg = "INSERT(school): {}".format(str(len(result_list)))
        return msg

    def with_scheme(self, table):
        return self.db_name + "." + table

"""
基幹DB用クラス
"""
class CoreDB(MariaDB):

    def __init__(self, config=None, db_host='localhost'):
        super().__init__(config, db_host)
        self.db_host = db_host
        self.db_port = 3306
        self.db_name = 'db_name'
        self.db_user = 'db_user'
        self.db_pass = 'db_pass'
        self.load_config(config)

"""
オンラインDB用クラス
"""
class OpDB(CoreDB):
    
    def __init__(self, config=None, db_host='localhost'):
        super().__init__(config, db_host)
        self.db_name = 'bh_learning_db'
        self.load_config(config)

"""
分析DB用クラス
"""
class AnalyticsDB(CoreDB):
    
    def __init__(self, config=None, db_host='localhost'):
        super().__init__(config, db_host)
        self.db_name = 'MB_analytics_db'
        self.load_config(config)
        #print(self.db_host,self.db_name,self.db_pass,self.db_port)



"""
SQLite操作用クラス
"""
class SQLite():
    def __init__(self, table_struct, exec_dir=None):
        self.db_file = table_struct['db_file']
        self.table = table_struct['name']
        self.exec_dir = exec_dir
        self.connection = None

    def create_table(self, table_struct=None, is_recreate=False):
        if is_recreate:
            print('Recreate Table.')
            self.drop_table(table_struct['name'])
        self.do_sql(self.get_sql_for_create_table(table_struct))

    def get_abs_path(self, file_path):
        if self.exec_dir:
            return self.exec_dir + '/' + file_path
        return file_path

    def get_conn(self):
        return sqlite3.connect(self.get_abs_path(self.db_file))

    def fetch_one(self, sql):
        res = None
        try:
            self.conn = self.get_conn()
            cursor = self.conn.cursor()
            res = cursor.execute(sql)
            return res.fetchone()
        except Exception as e:
            t, v, tb = sys.exc_info()
            pprint(traceback.format_exception(t,v,tb))
            pprint(traceback.format_tb(e.__traceback__))
        cursor.close()
        return res

    def fetch_all(self, sql):
        res = None
        try:
            self.conn = self.get_conn()
            cursor = self.conn.cursor()
            res = cursor.execute(sql)
            return res.fetchall()
        except Exception as e:
            t, v, tb = sys.exc_info()
            pprint(traceback.format_exception(t,v,tb))
            pprint(traceback.format_tb(e.__traceback__))
        cursor.close()
        return res

    def fetch_all_by_col(self, sql, col_list):
        list = self.fetch_all(sql)
        res = []
        if not list:
            return res
        for row in list:
            dict = {}
            for idx,col in enumerate(col_list):
                dict[col] = row[idx]
            res.append(dict)
        return res

    def fetch_all_with_col(self, sql, col_list):
        return self.fetch_all_by_col(sql, col_list)

    def fetch_all_to_list(self, sql):
        list = self.fetch_all(sql)
        if not list:
            return []
        return [l[0] for l in list]

    def update_by_sql(self, sql):
        res = None
        try:
            self.conn = self.get_conn()
            cursor = self.conn.cursor()
            res = cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            t, v, tb = sys.exc_info()
            pprint(traceback.format_exception(t,v,tb))
            pprint(traceback.format_tb(e.__traceback__))
        cursor.close()
        return res

    def insert_many(self, table, col_list, value_list):
        res = None
        try:
            self.conn = self.get_conn()
            cursor = self.conn.cursor()
            placeholder = ','.join(['?' for col in col_list])
            sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table, ','.join(col_list), placeholder)
            for value in value_list:
                cursor.execute(sql, value)
            self.conn.commit()
            res = 'InsertResults: {}'.format(str(len(value_list)))
        except Exception as e:
            t, v, tb = sys.exc_info()
            pprint(traceback.format_exception(t,v,tb))
            pprint(traceback.format_tb(e.__traceback__))
            cursor.close()
            sys.exit()
        cursor.close()
        return res

    def truncate_table(self, table):
        sql = "TRUNCATE TABLE " + table
        self.do_sql(sql)
        print(sql)

    def get_sql_for_create_table(self, table):
        # SQLiteとMariaDBの型変換表
        cnv_list = {
            'int': 'INTEGER', 
            'varchar': 'TEXT', 
            #'datetime': 'TEXT', 
            #'date': 'TEXT',
            'TIMESTAMP': 'DATETIME'
        }
        sql_base = "CREATE TABLE IF NOT EXISTS `{}` ({});"
        col_list = []
        for col in table['columns']:
            type = col['type']
            for cnv, change_type in cnv_list.items():
                if cnv in col['type']:
                    type = change_type
                    break
            col_list.append(
                "`{}` {} {}".format(
                    col['name'],
                    type,
                    col['option'] if 'option' in col.keys() else ''
                )
            )
        sql_part_col = ",\n".join(col_list)
        sql = sql_base.format(
            table['name'],
            sql_part_col
        )
        #print(sql)
        return sql

    def do_sql(self, sql):
        res = None
        try:
            self.conn = self.get_conn()
            cursor = self.conn.cursor()
            res = cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            t, v, tb = sys.exc_info()
            pprint(traceback.format_exception(t,v,tb))
            pprint(traceback.format_tb(e.__traceback__))
        cursor.close()
        return res

    def drop_table(self, table):
        drop_sql = "DROP TABLE IF EXISTS {}".format(table)
        res = self.do_sql(drop_sql)
        print(drop_sql)
        return res

if __name__ == '__main__':
    core_db = CoreDB()
    list = core_db.fetch_all("SELECT id_c, email_c FROM user_t limit 10")
    print(list)
    an_db = AnalyticsDB()
    list = an_db.fetch_all("SELECT id_c, area_c from test_center_t limit 10")
    print(list)
    list = core_db.fetch_all_df("SELECT * from user_t limit 10")
    print("---DataFrame---")
    print(list)
