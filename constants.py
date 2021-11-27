class Constants:
    sql_create_mau_hy = 'CREATE TABLE IF NOT EXISTS mau_hy_month (' \
                        'id BIGINT(20) NOT NULL AUTO_INCREMENT, ' \
                        'data_type VARCHAR(100) NOT NULL, ' \
                        'dt_year INT(4) NOT NULL, ' \
                        'dt_month INT(2) NOT NULL, ' \
                        'branch_id VARCHAR(20) NOT NULL, ' \
                        'branch_nm VARCHAR(100) NOT NULL, ' \
                        'dfgz_sign_org_id VARCHAR(20) NOT NULL, ' \
                        'dfgz_sign_org_nm VARCHAR(100) NOT NULL, ' \
                        'corp_cst_id VARCHAR(100) NOT NULL, ' \
                        'corp_cst_nm VARCHAR(200) NOT NULL, ' \
                        'corp_industry VARCHAR(100) NOT NULL, ' \
                        'mpb_sign_pnum INT(10) NOT NULL, ' \
                        'dfgz_pnum INT(10) NOT NULL, ' \
                        'dfgz_date DATE NOT NULL, ' \
                        'day_01 INT(10) NOT NULL, ' \
                        'day_02 INT(10) NOT NULL, ' \
                        'day_03 INT(10) NOT NULL, ' \
                        'day_04 INT(10) NOT NULL, ' \
                        'day_05 INT(10) NOT NULL, ' \
                        'day_06 INT(10) NOT NULL, ' \
                        'day_07 INT(10) NOT NULL, ' \
                        'day_08 INT(10) NOT NULL, ' \
                        'day_09 INT(10) NOT NULL, ' \
                        'day_10 INT(10) NOT NULL, ' \
                        'day_11 INT(10) NOT NULL, ' \
                        'day_12 INT(10) NOT NULL, ' \
                        'day_13 INT(10) NOT NULL, ' \
                        'day_14 INT(10) NOT NULL, ' \
                        'day_15 INT(10) NOT NULL, ' \
                        'day_16 INT(10) NOT NULL, ' \
                        'day_17 INT(10) NOT NULL, ' \
                        'day_18 INT(10) NOT NULL, ' \
                        'day_19 INT(10) NOT NULL, ' \
                        'day_20 INT(10) NOT NULL, ' \
                        'day_21 INT(10) NOT NULL, ' \
                        'day_22 INT(10) NOT NULL, ' \
                        'day_23 INT(10) NOT NULL, ' \
                        'day_24 INT(10) NOT NULL, ' \
                        'day_25 INT(10) NOT NULL, ' \
                        'day_26 INT(10) NOT NULL, ' \
                        'day_27 INT(10) NOT NULL, ' \
                        'day_28 INT(10) NOT NULL, ' \
                        'day_29 INT(10) NOT NULL, ' \
                        'day_30 INT(10) NOT NULL, ' \
                        'day_31 INT(10) NOT NULL, ' \
                        'PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=gbk;'

    sql_create_mau_hy_target = 'CREATE TABLE IF NOT EXISTS mau_hy_target (' \
                               'id BIGINT(20) NOT NULL AUTO_INCREMENT, ' \
                               'dt_year INT(4) NOT NULL, ' \
                               'dt_month INT(2) NOT NULL, ' \
                               'name_level VARCHAR(100) NOT NULL, ' \
                               'branch_nm VARCHAR(100) NOT NULL, ' \
                               'target_login_pnum INT(10) NOT NULL, ' \
                               'target_login_rate DOUBLE(10,2) NOT NULL, ' \
                               'target_live_pnum INT(10) NOT NULL, ' \
                               'target_live_rate DOUBLE(10,2) NOT NULL, ' \
                               'ptarget_login_pnum INT(10) NOT NULL, ' \
                               'ptarget_login_rate DOUBLE(10,2) NOT NULL, ' \
                               'ptarget_live_pnum INT(10) NOT NULL, ' \
                               'ptarget_live_rate DOUBLE(10,2) NOT NULL, ' \
                               'PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=gbk;'

    sql_create_mau_summary_branch = 'CREATE TABLE IF NOT EXISTS mau_hy_summary_branch (' \
                                    'id BIGINT(20) NOT NULL AUTO_INCREMENT, ' \
                                    'data_type VARCHAR(100) NOT NULL, ' \
                                    'dt_year INT(4) NOT NULL, ' \
                                    'dt_month INT(2) NOT NULL, ' \
                                    'name_level VARCHAR(100) NOT NULL, ' \
                                    'name_label VARCHAR(100) NOT NULL, ' \
                                    'corp_cst_nm INT(10) NOT NULL, ' \
                                    'dfgz_pnum INT(10) NOT NULL, ' \
                                    'mpb_sign_pnum INT(10) NOT NULL, ' \
                                    'login_pnum INT(10) NOT NULL, ' \
                                    'live_pnum INT(10) NOT NULL, ' \
                                    'phone_rate DOUBLE(10,2) NOT NULL, ' \
                                    'login_rate DOUBLE(10,2) NOT NULL, ' \
                                    'live_rate DOUBLE(10,2) NOT NULL, ' \
                                    'pcorp_cst_nm INT(10) NOT NULL, ' \
                                    'pdfgz_pnum INT(10) NOT NULL, ' \
                                    'pmpb_sign_pnum INT(10) NOT NULL, ' \
                                    'plogin_pnum INT(10) NOT NULL, ' \
                                    'plive_pnum INT(10) NOT NULL, ' \
                                    'pphone_rate DOUBLE(10,2) NOT NULL, ' \
                                    'plogin_rate DOUBLE(10,2) NOT NULL, ' \
                                    'plive_rate DOUBLE(10,2) NOT NULL, ' \
                                    'PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=gbk;'

    cols_mau_summary_branch = ['data_type',
                               'dt_year',
                               'dt_month',
                               'name_level',
                               'name_label',
                               'corp_cst_nm',
                               'dfgz_pnum',
                               'mpb_sign_pnum',
                               'login_pnum',
                               'live_pnum',
                               'phone_rate',
                               'login_rate',
                               'live_rate',
                               'pcorp_cst_nm',
                               'pdfgz_pnum',
                               'pmpb_sign_pnum',
                               'plogin_pnum',
                               'plive_pnum',
                               'pphone_rate',
                               'plogin_rate',
                               'plive_rate']

    dict_mau_hy_colnames = {'分行编号': 'branch_id',
                            '分行名称': 'branch_nm',
                            '签约机构编号': 'sign_org_id',
                            '签约机构名称': 'sign_org_nm',
                            '企业名称': 'corp_nm',
                            '企业编号': 'corp_id',
                            '行业类别': 'industry_type',
                            '手机银行签约人数': 'mpb_sign_pnum',
                            '代发人数': 'dfgz_pnum',
                            '代发日期': 'dfgz_date'}

    dict_mau_hy_colnames_reverse = {v: k for k, v in dict_mau_hy_colnames.items()}

    days_num_cols = ['day_01', 'day_02', 'day_03', 'day_04', 'day_05', 'day_06', 'day_07', 'day_08', 'day_09', 'day_10',
                     'day_11', 'day_12', 'day_13', 'day_14', 'day_15', 'day_16', 'day_17', 'day_18', 'day_19', 'day_20',
                     'day_21', 'day_22', 'day_23', 'day_24', 'day_25', 'day_26', 'day_27', 'day_28', 'day_29', 'day_30',
                     'day_31']
