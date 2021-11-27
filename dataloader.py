import os
from datetime import datetime, date
import pandas as pd
import time
from config import Config
from mysqlpool import MysqlPool
from clean_mauhy import CleanMauHy


class DataLoader:

    def __init__(self):
        self._dbpool = MysqlPool()
        self._mau_hy_name = Config.MYSQL_TABLE['MAU_HY_MONTH']
        self._mau_hy_summary = Config.MYSQL_TABLE['MAU_HY_SUMMARY_BRANCH']
        print('-------- 完成初始化 --------')

    def start(self):
        time_start = datetime.now()
        print('---------- 开始时间：%s ------------' % time_start)
        self.search_fold()

        for one in range(5):
            time.sleep(1)
            print(one)
        time_close = datetime.now()
        time_used = time_close - time_start
        print('---------- 结束时间：%s，用时：%s ------------' % (time_start, time_used))

    def search_fold(self):
        for one in os.listdir(Config.DATAIN_DIR):
            filenamefull = os.path.join(Config.DATAIN_DIR, one)
            print(filenamefull)
            if os.path.isfile(filenamefull):
                list_one = os.path.splitext(one)
                name = list_one[0]
                file_type = list_one[-1]
                table_exist = self.checkTableExist(name)
                if table_exist and (file_type == '.csv'):
                    df = pd.read_csv(filenamefull, encoding='gbk')
                    dt_year = df['dt_year'][0]
                    dt_month = df['dt_month'][0]
                    if name == self._mau_hy_name:
                        self.processMauHyDataframe(name, df, dt_year, dt_month)
                    else:
                        self.upsertDatabase(name, df, dt_year, dt_month)

    def checkTableExist(self, tablename):
        sql = '''SELECT table_name FROM information_schema.TABLES WHERE table_name ='%s';''' % tablename
        res = self._dbpool.fetch_one(sql)
        print('查询是否存在数据表：%s' % res)
        return res

    def processMauHyDataframe(self, name, dataframe, dt_year, dt_month):
        cleaner = CleanMauHy(dataframe)
        dataframe_need = cleaner.makeSummary(dt_year, dt_month)
        self.upsertDatabase(self._mau_hy_summary, dataframe_need, dt_year, dt_month)
        self.upsertDatabase(name, dataframe, dt_year, dt_month)

    def upsert_database_one_by_one(self, tablename, dataframe, dt_year, dt_month):
        sql_del = '''DELETE FROM %s WHERE dt_year=%s AND dt_month=%s;''' % (tablename, dt_year, dt_month)
        self._dbpool.execute(sql_del)
        dataframe.fillna('', inplace=True)
        keys = list(dataframe.columns)
        values = list(dataframe.values)
        for i in range(len(values)):
            value = values[i]
            row = dict(zip(keys, value))
            print(row)
            self._dbpool.table_insert(tablename, row)

    def upsertDatabase(self, tablename, dataframe, dt_year, dt_month):
        sql_del = '''DELETE FROM %s WHERE dt_year=%s AND dt_month=%s;''' % (tablename, dt_year, dt_month)
        self._dbpool.execute(sql_del)
        if 'dfgz_date' in list(dataframe.columns):
            date_default = date(dt_year, dt_month, 1)
            dataframe['dfgz_date'].fillna(date_default, inplace=True)
        dataframe.fillna('', inplace=True)
        print(tablename)
        print(dataframe.head())
        self._dbpool.table_df_insertmany(tablename, dataframe)


if __name__ == '__main__':
    DataLoader().start()
