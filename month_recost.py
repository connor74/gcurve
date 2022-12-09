import pandas as pd
import numpy as np
import datetime
import requests
import json
import os
import pathlib

MAIN_DIR = "//s04/Moex/"

path_report = pathlib.Path(MAIN_DIR)


class MonthRecost:

    def __init__(self,
                 last_date=(datetime.date.today() - datetime.timedelta(days=1)).isoformat(),
                 days=25,
                 dynamic_link='http://moex.com/iss/downloads/engines/stock/zcyc/dynamic.csv.zip'):
        self.params = {
            'date': last_date,
        }
        self.max_date = last_date
        self.date = last_date
        self.max_imported_date = ''
        self.min_imported_date = ''
        self.nunique_dates = 0
        self.days = days
        self.dynamic_link = dynamic_link
        self.history_df = pd.DataFrame()
        self.params_df = pd.DataFrame()
        self.dynamic = pd.DataFrame()
        self.df_all = pd.DataFrame()
        self.path = './media/'

    def _read_bonds_params(self):
        link = 'http://iss.moex.com/iss/engines/stock/markets/bonds/securities.json'
        jsn = requests.get(link).json()
        df = pd.DataFrame(data=jsn['securities']['data'], columns=jsn['securities']['columns'])
        dates_columns = ['PREVDATE', 'BUYBACKDATE', 'OFFERDATE', 'SETTLEDATE', 'NEXTCOUPON']
        df = df.apply(lambda x: x.replace('0000-00-00', pd.NaT) if x.name in dates_columns else x)
        df = df.apply(lambda x: pd.to_datetime(x, infer_datetime_format=True) if x.name in dates_columns else x)
        self.params_df = df.copy()

    def _read_history_data(self):
        def minus_day(str_date):
            date = datetime.date.fromisoformat(str_date)
            date -= datetime.timedelta(days=1)
            return datetime.date.isoformat(date)

        link = 'http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities.json'
        df = pd.DataFrame()
        while self.days > 0:
            self.params['start'] = 1
            while True:
                r = requests.get(link, self.params)
                data = r.json()['history']['data']
                if not data and self.params['start'] == 1:
                    self.params['date'] = minus_day(self.params['date'])
                    break
                elif not data and self.params['start'] != 1:
                    self.params['date'] = minus_day(self.params['date'])
                    self.days -= 1
                    break
                else:
                    df = pd.concat([df, pd.DataFrame(data)])
                    self.params['start'] += 100
            print(self.days)

        df.columns = r.json()['history']['columns']

        dates_columns = ['TRADEDATE', 'MATDATE', 'OFFERDATE', 'BUYBACKDATE', 'LASTTRADEDATE']
        df = df.apply(lambda x: pd.to_datetime(x) if x.name in dates_columns else x)
        float_columns = ['IRICPICLOSE', 'BEICLOSE', 'COUPONPERCENT', 'CBRCLOSE', 'YIELDLASTCOUPON']
        df = df.apply(lambda x: pd.to_numeric(x) if x.name in float_columns else x)
        df['ISDEALS'] = np.where(df['NUMTRADES'] > 0, 1, 0)

        self.history_df = df.copy()
        self.min_imported_date = datetime.date.isoformat(self.history_df.TRADEDATE.min())
        self.max_imported_date = datetime.date.isoformat(self.history_df.TRADEDATE.max())
        self.nunique_dates = self.history_df.TRADEDATE.nunique()

    def _read_dynamic(self):
        if self.dynamic.empty:
            file = requests.get(self.dynamic_link)
            open(f"{self.path}d.zip", 'wb').write(file.content)
            dynamic = pd.read_csv(f"{self.path}d.zip", skiprows=2, sep=';', decimal=',')
            dynamic['tradedate'] = pd.to_datetime(dynamic['tradedate'], format="%d.%m.%Y")
            self.dynamic = dynamic

    def get_data(self):
        self._read_bonds_params()
        self._read_history_data()
        self._read_dynamic()
        df = self.history_df.copy()
        params = self.params_df.copy()

        df.sort_values(['SHORTNAME', 'TRADEDATE'], inplace=True)
        gr_df = df.groupby(['SECID', 'SHORTNAME']).agg({
            'BOARDID': 'last',
            'MATDATE': 'max',
            'OFFERDATE': 'max',
            'NUMTRADES': 'sum',
            'MARKETPRICE3': 'last',
            'ACCINT': 'last',
            'COUPONVALUE': 'last',
            'VALUE': 'sum',
            'VOLUME': 'sum',
            'ISDEALS': 'sum',
            'TRADEDATE': 'last',
            'FACEVALUE': 'last'
        }).reset_index()

        gr_df = pd.merge(gr_df, params[['ISIN', 'SECID', 'COUPONPERIOD', 'REGNUMBER', 'ISSUESIZE', 'NEXTCOUPON']], \
                         how='left', on='SECID')
        gr_df['PER'] = gr_df['VALUE'] / (gr_df['FACEVALUE'] * gr_df['ISSUESIZE']) * 100
        gr_df['RESULT'] = np.where((gr_df['PER'] >= 0.1) & (gr_df['NUMTRADES'] >= 10) & (gr_df['ISDEALS'] >= 5), \
                                   1, 0)

        self.df_all = gr_df
        print(f"Первая дата экспорта: {self.min_imported_date}")
        print(f"Последняя дата экспорта: {self.max_imported_date}\n")
        print(f"Количество торговых дней: {self.nunique_dates}\n")

    def export_data_excel(self):
        try:
            writer = pd.ExcelWriter(pathlib.Path(path_report, f"result_all_{self.max_date}.xlsx"), engine='xlsxwriter',
                                    datetime_format='dd.mm.yyyy')
            self.df_all[['ISIN', 'SECID', 'SHORTNAME', 'MATDATE', 'ACCINT', 'MARKETPRICE3', 'COUPONVALUE', 'NEXTCOUPON',
                         'COUPONPERIOD', 'BOARDID', 'FACEVALUE', 'REGNUMBER', 'OFFERDATE', 'RESULT', 'TRADEDATE']]. \
                sort_values(by=['SHORTNAME']). \
                to_excel(writer, sheet_name='Sheet1', index=False)
            self.dynamic.to_excel(writer, sheet_name='Sheet2', index=False)
            #writer.save()

        except PermissionError:
            print(f"Ошибка! Возможно открыт файл: {self.path}result_all_{self.max_date}.xlsx")

        try:
            with pd.ExcelWriter(pathlib.Path(path_report, "result_all.xlsx"), engine='xlsxwriter', datetime_format='dd.mm.yyyy') as writer:
                self.df_all[['ISIN', 'SECID', 'SHORTNAME', 'MATDATE', 'ACCINT', 'MARKETPRICE3', 'COUPONVALUE', 'NEXTCOUPON',
                             'COUPONPERIOD', 'BOARDID', 'FACEVALUE', 'REGNUMBER', 'OFFERDATE', 'RESULT', 'TRADEDATE']]. \
                    sort_values(by=['SHORTNAME']). \
                    to_excel(writer, sheet_name='Sheet1', index=False)
                self.dynamic.to_excel(writer, sheet_name='Sheet2', index=False)
            #writer.save()

        except PermissionError:
            print(f"Ошибка! Возможно открыт файл: {self.path}result_all.xlsx")



if __name__ == '__main__':
    m = MonthRecost()
    m.get_data()
    m.export_data_excel()
