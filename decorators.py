from functools import wraps
import time


# 使用函数装饰器singleton实现单列
def singleton(cls):
    _instances = {}
    # 装饰器 作用于类 cls
    # 使用不可变的类地址作为键，其实例作为值，
    # 每次创造实例时，首先查看该类是否存在实例，存在的话直接返回该实例即可，
    # 否则新建一个实例并存放在字典中。
    @wraps(cls)
    def instance(*args, **kw):
        if cls not in _instances:
            _instances[cls] = cls(*args, **kw)
        return _instances[cls]
    return instance


def timer(func):
    def decor(*args):
        start_time = time.time()
        print('-------开始%s方法：%s----------' % (func, start_time))
        func(*args)
        end_time = time.time()
        d_time = end_time - start_time
        print('-------结束%s方法：%s，用时：%s----------' % (func, end_time, d_time))
    return decor

