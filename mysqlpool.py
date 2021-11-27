import pymysql
import traceback
from decorators import timer
from dbutils.pooled_db import PooledDB, SharedDBConnection
from pandas.core.frame import DataFrame
from config import Config


class MysqlPool:

    def __init__(self):
        self.dbparams = Config.MYSQL_DICT
        self.POOL = PooledDB(
            creator=pymysql,            # 使用链接数据库的模块
            maxconnections=None,        # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=2,                # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=10,               # 链接池中最多闲置的链接，0和None不限制
            maxshared=3,                # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
            blocking=True,              # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,              # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],              # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            ping=0,                     # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            host=self.dbparams['host'],
            port=self.dbparams['port'],
            user=self.dbparams['user'],
            password=self.dbparams['password'],
            database=self.dbparams['db'],
            charset='gbk'
        )

    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            cls._instance = object.__new__(cls)
        return cls._instance

    def connect(self):
        conn = self.POOL.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        return conn, cursor

    def close(self, conn, cursor):
        cursor.close()
        conn.close()

    def execute(self, sql, param=None, autoclose=False):
        conn, cursor = self.connect()   # 从连接池获取连接
        count = 0                       # count : 为改变的数据条数
        try:
            if param:
                count = cursor.execute(sql, param)
            else:
                count = cursor.execute(sql)
            conn.commit()
            if autoclose:
                self.close(cursor, conn)
        except Exception as e:
            if e.args[0] == 1062:
                pass
            else:
                traceback.print_exc()
                raise e
        return cursor, conn, count

    def executemany(self, sql, values, autoclose=False):
        conn, cursor = self.connect()   # 从连接池获取连接
        count = 0                       # count : 为改变的数据条数
        try:
            count = cursor.executemany(sql, values)
            conn.commit()
            if autoclose:
                self.close(cursor, conn)
        except Exception as e:
            if e.args[0] == 1062:
                pass
            else:
                traceback.print_exc()
                raise e
        return cursor, conn, count

    def fetch_all(self, sql, args=None):
        cursor, conn, count = self.execute(sql, args)
        return cursor.fetchall()

    def fetch_one(self, sql, args=None):
        cursor, conn, count = self.execute(sql, args)
        return cursor.fetchone()


    ## =============== high level method for table ===================
    # table_has() 查询某个值是否存在于表中。查询的字段最好建立的在MySQL中建立了索引，不然数据量稍大就会很慢。
    def table_has(self, table_name, field, value):
        sql = 'SELECT %s FROM %s WHERE %s="%s" LIMIT 1' % (field, table_name, field, value)
        d = self.fetch_one(sql)
        return d

    # table_insert() 把一个字典类型的数据插入表中。字典的key必须是表的字段。
    def table_insert(self, table_name, item):
        fields = list(item.keys())
        values = list(item.values())
        fieldstr = ','.join(fields)
        valstr = ','.join(['%s'] * len(item))
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fieldstr, valstr)
        last_id = self.execute(sql, values)
        return last_id

    @timer
    def table_df_insertmany(self, table_name, dataframe: DataFrame):
        fields = list(dataframe.columns)
        values = []
        for one in dataframe.values:
            list_one = []
            for val in one:
                if isinstance(val, str):
                    val = val.encode('gbk')
                list_one.append(val)
            tuple_one = tuple(list_one)
            values.append(tuple_one)
        fieldstr = ','.join(fields)
        valstr = ','.join(['%s'] * len(fields))
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fieldstr, valstr)
        self.executemany(sql, values)

    # table_update() 更新表中的一条记录。其中, field_where最好是建立了索引，不然数据量稍大就会很慢。
    def table_update(self, table_name, updates, field_where, value_where):
        upsets = []
        values = []
        for k, v in updates.items():
            s = '%s=%%s' % k
            upsets.append(s)
            values.append(v)
        upsets = ','.join(upsets)
        sql = 'UPDATE %s SET %s WHERE %s="%s"' % (table_name, upsets, field_where, value_where,)
        self.execute(sql, values)

