import os


class Config:

    GROUP_NAME = 'ccbbank'
    PROJECT_NAME = 'mau_hy_month'

    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    ROOT_DIR = os.path.dirname(BASE_DIR)
    DATAIN_DIR = 'D:/CcbDatain'
    os.makedirs(DATAIN_DIR, exist_ok=True)

    HOST_LOCAL = 'localhost'

    MYSQL_DICT = {
        'host': HOST_LOCAL,
        'port': 3306,
        'db': GROUP_NAME,
        'user': 'root',
        'password': '123456',
    }

    MYSQL_TABLE = {
        'MAU_HY_MONTH': 'mau_hy_month',
        'MAU_HY_TARGET': 'mau_hy_target',
        'MAU_HY_SUMMARY_BRANCH': 'mau_hy_summary_branch',
    }


