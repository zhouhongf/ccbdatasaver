import pandas as pd
import calendar
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from pandas.core.frame import DataFrame
from constants import Constants


class CleanMauHy(object):

    def __init__(self, dataframe: DataFrame):
        self._yearmonth = (date.today() - relativedelta(months=+1)).strftime('%Y%m')
        self._dt_year = int(self._yearmonth[0:4])
        self._dt_month = int(self._yearmonth[4:6])
        self._date_start, self._date_close, self._day_num = self.getMonthStartCloseDates(self._yearmonth)
        self._dataframe = dataframe.copy()

        self._dataframe_mau_accum = pd.DataFrame([])
        self._dataframe_mau_net = pd.DataFrame([])
        self._dataframe_hy_accum = pd.DataFrame([])
        self._dataframe_hy_net = pd.DataFrame([])
        self._dataframe_mau_accum_p = pd.DataFrame([])
        self._dataframe_mau_net_p = pd.DataFrame([])
        self._dataframe_hy_accum_p = pd.DataFrame([])
        self._dataframe_hy_net_p = pd.DataFrame([])

        self.makeDataframe(self._dataframe)
        self._industry_need = self.getIndustryNeed()
        self._cols_summary = Constants.cols_mau_summary_branch
        print('--------初始化CleanMauHy----------')

    def getMonthStartCloseDates(self, yearmonth: str):
        date_full = yearmonth + '01'
        date_start = datetime.strptime(date_full, '%Y%m%d')
        days_num = calendar.monthrange(date_start.year, date_start.month)[1]
        date_close = date_start + timedelta(days=days_num)
        date_start_str = date_start.strftime('%Y%m%d')
        date_close_str = date_close.strftime('%Y%m%d')
        return date_start_str, date_close_str, days_num

    # 暂时用不着
    def transferToAccum(self, dataframe):
        cols = list(dataframe.columns)
        day_01_col_num = cols.index('day_01')
        day_31_col_num = cols.index('day_31')
        dataframe_head = dataframe.iloc[:, :day_01_col_num+1]
        for i in range(day_01_col_num+1, day_31_col_num+1):
            col_name_pre = cols[i-1]
            col_name = cols[i]
            dataframe_head[col_name] = dataframe_head[col_name_pre] + dataframe[col_name]
        return dataframe_head

    def makeDataframe(self, dataframe: DataFrame):
        dataframe.drop(columns=['dt_year', 'dt_month'], axis=1, inplace=True)

        self._dataframe_mau_accum = dataframe[dataframe['data_type'] == '日净MAU']
        self._dataframe_mau_accum.drop(columns=['data_type'], axis=1, inplace=True)
        self._dataframe_mau_net = dataframe[dataframe['data_type'] == '日MAU']
        self._dataframe_mau_net.drop(columns=['data_type'], axis=1, inplace=True)
        self._dataframe_hy_accum = dataframe[dataframe['data_type'] == '日净HY']
        self._dataframe_hy_accum.drop(columns=['data_type'], axis=1, inplace=True)
        self._dataframe_hy_net = dataframe[dataframe['data_type'] == '日HY']
        self._dataframe_hy_net.drop(columns=['data_type'], axis=1, inplace=True)

        self._dataframe_mau_accum_p = dataframe[dataframe['data_type'] == '日净MAU工资单']
        self._dataframe_mau_accum_p.drop(columns=['data_type'], axis=1, inplace=True)
        self._dataframe_mau_net_p = dataframe[dataframe['data_type'] == '日MAU工资单']
        self._dataframe_mau_net_p.drop(columns=['data_type'], axis=1, inplace=True)
        self._dataframe_hy_accum_p = dataframe[dataframe['data_type'] == '日净HY工资单']
        self._dataframe_hy_accum_p.drop(columns=['data_type'], axis=1, inplace=True)
        self._dataframe_hy_net_p = dataframe[dataframe['data_type'] == '日HY工资单']
        self._dataframe_hy_net_p.drop(columns=['data_type'], axis=1, inplace=True)

    def getIndustryNeed(self):
        datacopy = self._dataframe_mau_accum.copy()
        datagroup = datacopy.groupby(['corp_industry'], as_index=False).agg({'dfgz_pnum': 'sum'})
        dataneed = datagroup[datagroup['dfgz_pnum'] > 10000]
        labelneed = list(dataneed['corp_industry'].values)
        return labelneed

    def makeSummary(self, dt_year, dt_month):
        data_branch, data_industry = self.makeSummaryData('苏州分行', dt_year, dt_month)
        list_dataframe = [data_branch, data_industry]
        for one in set(list(self._dataframe_mau_accum['branch_nm'])):
            df_branch, df_industry = self.makeSummaryData(one, dt_year, dt_month)
            list_dataframe.append(df_branch)
            list_dataframe.append(df_industry)
        dataframe_need = pd.concat(list_dataframe)
        dataframe_need.reset_index(drop=True, inplace=True)
        return dataframe_need

    def makeSummaryData(self, name_level, dt_year, dt_month):
        branch_label = 'branch_nm'
        if name_level != '苏州分行':
            branch_label = 'dfgz_sign_org_nm'
        dataframe_branch = self.prepareSummaryTable(group_label=branch_label, name_level=name_level, industry_limit=False)
        dataframe_industry = self.prepareSummaryTable(group_label='corp_industry', name_level=name_level, industry_limit=True)
        dataframe_branch_need = self.addDataframeTypeYearMonth(dataframe_branch, '分支行', name_level, dt_year, dt_month)
        dataframe_industry_need = self.addDataframeTypeYearMonth(dataframe_industry, '行业', name_level, dt_year, dt_month)
        return dataframe_branch_need, dataframe_industry_need

    def addDataframeTypeYearMonth(self, dataframe, data_type, name_level, dt_year, dt_month):
        dataframe['data_type'] = data_type
        dataframe['name_level'] = name_level
        dataframe['dt_year'] = dt_year
        dataframe['dt_month'] = dt_month
        dataframe_need = dataframe[self._cols_summary]
        return dataframe_need

    def prepareSummaryTable(self, group_label, name_level, industry_limit=False):
        data_mau_accum = self._dataframe_mau_accum.copy()
        data_hy_accum = self._dataframe_hy_accum.copy()
        data_mau_accum_p = self._dataframe_mau_accum_p.copy()
        data_hy_accum_p = self._dataframe_hy_accum_p.copy()
        if name_level != '苏州分行':
            data_mau_accum = self._dataframe_mau_accum[self._dataframe_mau_accum['branch_nm'] == name_level]
            data_hy_accum = self._dataframe_hy_accum[self._dataframe_hy_accum['branch_nm'] == name_level]
            data_mau_accum_p = self._dataframe_mau_accum_p[self._dataframe_mau_accum_p['branch_nm'] == name_level]
            data_hy_accum_p = self._dataframe_hy_accum_p[self._dataframe_hy_accum_p['branch_nm'] == name_level]

        mau_accum = self.makeSummaryTable(data_mau_accum, data_hy_accum, group_label, industry_limit)
        mau_accum_p = self.makeSummaryTable(data_mau_accum_p, data_hy_accum_p, group_label, industry_limit)
        mau_accum_p.rename(
            columns={'corp_cst_nm': 'pcorp_cst_nm', 'mpb_sign_pnum': 'pmpb_sign_pnum', 'dfgz_pnum': 'pdfgz_pnum', 'login_pnum': 'plogin_pnum',
                     'live_pnum': 'plive_pnum', 'phone_rate': 'pphone_rate', 'login_rate': 'plogin_rate',
                     'live_rate': 'plive_rate'}, inplace=True)
        dataframe_target = pd.merge(mau_accum, mau_accum_p, on='name_label', how='left')
        dataframe_target.fillna(0, inplace=True)
        return dataframe_target

    def makeSummaryTable(self, data_mau_accum, data_hy_accum, group_label, industry_limit=False):
        if industry_limit:
            data_mau_accum = data_mau_accum[data_mau_accum['corp_industry'].isin(self._industry_need)]
            data_hy_accum = data_hy_accum[data_hy_accum['corp_industry'].isin(self._industry_need)]
        mau_accum = data_mau_accum[[group_label, 'corp_cst_nm', 'dfgz_pnum', 'mpb_sign_pnum', 'day_31']].groupby([group_label], as_index=False).agg(
            {'corp_cst_nm': pd.Series.nunique, 'dfgz_pnum': 'sum', 'mpb_sign_pnum': 'sum', 'day_31': 'sum'})
        mau_accum.rename(columns={'day_31': 'login_pnum'}, inplace=True)
        hy_accum = data_hy_accum[[group_label, 'day_31']].groupby([group_label], as_index=False).sum()
        hy_accum.rename(columns={'day_31': 'live_pnum'}, inplace=True)
        mau_accum = pd.merge(mau_accum, hy_accum)

        mau_accum['phone_rate'] = round(mau_accum['mpb_sign_pnum'] / mau_accum['dfgz_pnum'] * 100, 2)
        mau_accum['login_rate'] = round(mau_accum['login_pnum'] / mau_accum['dfgz_pnum'] * 100, 2)
        mau_accum['live_rate'] = round(mau_accum['live_pnum'] / mau_accum['dfgz_pnum'] * 100, 2)

        # 分行名称，企业人数，代发人数，手机银行签约人数，登录人数，活跃人数，签约率，登录率，活跃率
        mau_accum.loc['total'] = ['合计', 0, 0, 0, 0, 0, 0, 0, 0]
        if len(data_mau_accum) > 0 and len(data_hy_accum) > 0:
            mau_accum.reset_index(drop=True, inplace=True)

            mau_accum.iloc[-1, 1:] = mau_accum.iloc[:-1, 1:].apply(lambda x: x.sum())
            mau_accum.iloc[-1, 6] = round(mau_accum.iloc[-1, 3] / mau_accum.iloc[-1, 2] * 100, 2)
            mau_accum.iloc[-1, 7] = round(mau_accum.iloc[-1, 4] / mau_accum.iloc[-1, 2] * 100, 2)
            mau_accum.iloc[-1, 8] = round(mau_accum.iloc[-1, 5] / mau_accum.iloc[-1, 2] * 100, 2)
            mau_accum['dfgz_pnum'] = mau_accum['dfgz_pnum'].apply(lambda x: int(x))
            mau_accum['mpb_sign_pnum'] = mau_accum['mpb_sign_pnum'].apply(lambda x: int(x))
            mau_accum['login_pnum'] = mau_accum['login_pnum'].apply(lambda x: int(x))
            mau_accum['live_pnum'] = mau_accum['live_pnum'].apply(lambda x: int(x))

        mau_accum.rename(columns={group_label: 'name_label'}, inplace=True)
        return mau_accum
