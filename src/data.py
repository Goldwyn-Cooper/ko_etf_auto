# 표준
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd
import ast
# 서드파티
import pandas as pd
import requests

MARKETCAP='https://finance.naver.com/api/sise/etfItemList.nhn?etfType=0'
PRICE='https://api.finance.naver.com/siseJson.naver'

def get_marketcap_from_naver() -> pd.DataFrame:
    response = requests.get(MARKETCAP) 
    cols = ('itemcode', 'etfTabCode', 'itemname', 'amonut', 'marketSum')
    data = response.json().get('result').get('etfItemList')
    df = pd.DataFrame(data).dropna().loc[:, cols]
    kwds = '액티브|혼합|레버리지|2X|단기|금리|배당|3년|은행|BBIG|\
            인도|베트남|콜|TRF|닥100|P500|리츠|MSCI|R50|I300|\
            HANARO|SOL|ARIRANG'
    df.query(f'not itemname.str.contains("{kwds}")', inplace=True)
    df['category_marketSum_mean'] = df['etfTabCode'].apply(lambda x: df[df.etfTabCode == x].marketSum.mean())
    df['category_amonut_mean'] = df['etfTabCode'].apply(lambda x: df[df.etfTabCode == x].amonut.mean())
    expr = f'marketSum >= category_marketSum_mean\
            and amonut >= category_amonut_mean\
            and etfTabCode != 1'
    return df.query(expr).loc[:, ['itemcode', 'itemname']].reset_index(drop=True)

def get_price(symbol):
    td = dt.utcnow() + rd(hours=9)
    year_1 = td - rd(years=1)
    literal = requests.get(PRICE, params=dict(
        symbol=symbol,
        requestType=1,
        startTime=year_1.strftime('%Y%m%d'),
        endTime='20991231',
        timeframe='day'
    )).text.replace('\n', '').replace('\t', '')
    data = ast.literal_eval(literal)
    df = pd.DataFrame(data[1:], columns=data[0]).loc[:, ('고가', '저가', '종가')]
    df.columns = ['high', 'low', 'close']
    return df