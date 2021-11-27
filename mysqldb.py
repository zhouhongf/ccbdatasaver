import time
import logging
import traceback
import pymysql
import pymysql.cursors
from config import Config
from decorators import timer
from pandas.core.frame import DataFrame


class MySQLDatabase:

    # ===================================初始化数据库并建立连接=========================================
    # 一般只需要四个参数就可以建立连接了：
    # •	host：数据库地址，本节就是localhost
    # •	database： 数据库名
    # •	user： 数据库用户名
    # •	password：数据库用户的密码
    # 后面还有几个参数可酌情使用：
    # •	max_idle_time： MySQL server默认8小时闲置就会断开客户端的连接；这个参数告诉客户端闲置多长时间要重新连接；
    # •	time_zone: 这里默认时区为0区，你可以设置为自己的时区，比如东8区 +8:00;
    # •	charset：默认为utf8mb4，即支持moji字符的utf8;

    MYSQLDB = Config.MYSQL_DICT

    def __init__(self, host=None, database=None, user=None, password=None,
                 port=3306,
                 max_idle_time=7 * 3600, connect_timeout=10,
                 time_zone="+0:00", charset="gbk", sql_mode="TRADITIONAL"):
        self.host = host or self.MYSQLDB['host']
        self.database = database or self.MYSQLDB['db']
        print('====================MySQL host:', self.host)
        self.max_idle_time = float(max_idle_time)

        args = dict(use_unicode=True, charset=charset,
                    database=database or self.MYSQLDB['db'],
                    init_command=('SET time_zone = "%s"' % time_zone),
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=connect_timeout, sql_mode=sql_mode)
        args["user"] = user or self.MYSQLDB['user']
        args["passwd"] = password or self.MYSQLDB['password']

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in self.host:
            args["unix_socket"] = self.host
        else:
            self.socket = None
            pair = self.host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = self.host
                args["port"] = 3306
        if port:
            args['port'] = port or self.MYSQLDB['port']

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to MySQL on %s", self.host, exc_info=True)

    def _ensure_connected(self):
        if self._db is None or (time.time() - self._last_use_time > self.max_idle_time):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = pymysql.connect(**self._db_args)
        self._db.autocommit(True)
        # print('MySQL已连接上，参数是：%s' % self._db_args)

    # ===========================数据库普通操作，查询，插入=========================================
    # 使用query()来获取一组数据，返回的是一个list，其中每个item就是一个dict，跟get()返回的字典一样。
    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()

    # 使用get()获取一个数据，返回的是一个dict，key就是数据库表的字段；
    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            d = cursor.fetchone()
            return d
        finally:
            cursor.close()

    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            return cursor.lastrowid
        except Exception as e:
            if e.args[0] == 1062:
                pass
            else:
                traceback.print_exc()
                raise e
        finally:
            cursor.close()

    # 使用insert()或execute()，看源码就知道，insert就是execute的别名。
    insert = execute

    ## =============== high level method for table ===================
    # table_has() 查询某个值是否存在于表中。查询的字段最好建立的在MySQL中建立了索引，不然数据量稍大就会很慢。
    def table_has(self, table_name, field, value):
        if isinstance(value, str):
            value = value.encode('gbk')

        sql = 'SELECT %s FROM %s WHERE %s="%s"' % (field, table_name, field, value)
        d = self.get(sql)
        return d

    # table_insert() 把一个字典类型的数据插入表中。字典的key必须是表的字段。
    def table_insert(self, table_name, item):
        '''item is a dict : key is mysql table field'''
        fields = list(item.keys())
        values = list(item.values())

        fieldstr = ','.join(fields)
        valstr = ','.join(['%s'] * len(item))
        for i in range(len(values)):
            if isinstance(values[i], str):
                values[i] = values[i].encode('gbk')

        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fieldstr, valstr)
        try:
            last_id = self.execute(sql, *values)
            return last_id
        except Exception as e:
            if e.args[0] == 1062:
                # just skip duplicated item
                pass
            else:
                traceback.print_exc()
                print('sql:', sql)
                print('item:')
                for i in range(len(fields)):
                    vs = str(values[i])
                    if len(vs) > 300:
                        print(fields[i], ' : ', len(vs), type(values[i]))
                    else:
                        print(fields[i], ' : ', vs, type(values[i]))
                raise e

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
        cursor = self._cursor()
        try:
            cursor.executemany(sql, values)
        except Exception as e:
            if e.args[0] == 1062:
                pass
            else:
                traceback.print_exc()
                raise e
        finally:
            cursor.close()



    # table_update() 更新表中的一条记录。其中, field_where最好是建立了索引，不然数据量稍大就会很慢。
    def table_update(self, table_name, updates, field_where, value_where):
        '''updates is a dict of {field_update:value_update}'''
        upsets = []
        values = []
        for k, v in updates.items():
            s = '%s=%%s' % k
            upsets.append(s)
            values.append(v)
        upsets = ','.join(upsets)
        sql = 'UPDATE %s SET %s WHERE %s="%s"' % (
            table_name,
            upsets,
            field_where, value_where,
        )
        self.execute(sql, *(values))


if __name__ == '__main__':
    db = MySQLDatabase(
        'localhost',
        'db_name',
        'user',
        'password'
    )
    # 获取一条记录
    sql = 'select * from test_table where id=%s'
    data = db.get(sql, 2)

    # 获取多条记录
    sql = 'select * from test_table where id>%s'
    data = db.query(sql, 2)

    # 插入一条数据
    sql = 'insert into test_table(title, url) values(%s, %s)'
    last_id = db.execute(sql, 'test', 'http://a.com/')
    # 或者
    last_id = db.insert(sql, 'test', 'http://a.com/')

    # 使用更高级的方法插入一条数据
    item = {
        'title': 'test',
        'url': 'http://a.com/',
    }
    last_id = db.table_insert('test_table', item)



