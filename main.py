import sys
from sqlite3 import Connection
from typing import Any

import requests
import json
import sqlite3
import os.path
import pandas as pd


def create_db(name: str = "data", path: str = "") -> Connection:
    return sqlite3.connect(f'{path}{name}.db')


def create_history_table(con):
    cur = con.cursor()
    cur.execute(
        '''
        CREATE TABLE if not exists history (
            BOARDID TEXT,
            TRADEDATE TEXT,
            SHORTNAME TEXT,
            SECID TEXT,
            NUMTRADES INTEGER,
            VALUE INTEGER,
            LOW REAL,
            HIGH REAL,
            CLOSE REAL,
            LEGALCLOSEPRICE REAL,
            ACCINT REAL,
            WAPRICE REAL,
            YIELDCLOSE REAL,
            OPEN REAL,
            VOLUME REAL,
            MARKETPRICE2 REAL,
            MARKETPRICE3 REAL,
            ADMITTEDQUOTE REAL,
            MP2VALTRD REAL,
            MARKETPRICE3TRADESVALUE REAL,
            ADMITTEDVALUE REAL,
            MATDATE TEXT,
            DURATION REAL,
            YIELDATWAP REAL,
            IRICPICLOSE REAL,
            BEICLOSE REAL,
            COUPONPERCENT REAL,
            COUPONVALUE REAL,
            BUYBACKDATE TEXT,
            LASTTRADEDATE TEXT,
            FACEVALUE REAL,
            CURRENCYID TEXT,
            CBRCLOSE REAL,
            YIELDTOOFFER REAL,
            YIELDLASTCOUPON REAL,
            OFFERDATE TEXT,
            FACEUNIT TEXT,
            TRADINGSESSION INTEGER
        )
        '''
    )


def insert(table_name: str, data: list, con: Connection) -> None:
    nums = len(data[0])
    if data:
        cursor = con.cursor()
        sql = f'INSERT INTO {table_name} VALUES(' + '?,' * (nums - 1) + '?);'
        cursor.executemany(
            sql,
            data
        )
        con.commit()
        con.close()
    else:
        print("!!!!Data is empty")


class MOEXDecoder:
    def __init__(self, url: str, **params: dict[str, Any]) -> None:
        self.url = url
        self.params = params
        try:
            r = requests.get(self.url, self.params)
            if r.status_code != 200:
                raise Exception("Код ответа: ", r.status_code)
        except requests.ConnectionError as e:
            print("OOPS!! Connection Error.\n")
            print(str(e))
        except requests.RequestException as e:
            print("OOPS!! General Error")
            print(str(e))

        self.jsn = json.loads(r.text)

    def columns(self) -> list:
        return self.jsn['history']['columns']

    def data_list(self) -> list:
        data = self.jsn['history']['data']
        if data:
            return data
        else:
            return None

    def data_tuple(self) -> tuple:
        data = self.jsn['history']['data']
        if data:
            output_list = []
            for i in self.jsn['history']['data']:
                output_list.append(tuple(i))
            return output_list
        else:
            return None


code = "RU000A0JRVU3"
code1 = "dfdf"
link = f"http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/{code}.json"
data = MOEXDecoder(link).data_tuple()

con = create_db()
create_history_table(con)
insert('history', data, con)
