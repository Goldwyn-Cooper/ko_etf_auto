# 표준
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd
import ast
# 서드파티
import pandas as pd
import requests

pd.options.display.float_format = '{:.3f}'.format

MARKETCAP='https://finance.naver.com/api/sise/etfItemList.nhn?etfType=0'
PRICE='https://api.finance.naver.com/siseJson.naver'

def get_marketcap_from_naver() -> pd.DataFrame:
    response = requests.get(MARKETCAP) 
    cols = ('itemcode', 'etfTabCode', 'itemname', 'amonut', 'marketSum')
    data = response.json().get('result').get('etfItemList')
    df = pd.DataFrame(data).dropna().loc[:, cols]
    kwds = '액티브|혼합|합성|닥100|P500|MSCI'\
            +'|중국|차이나|인도|베트남|니케이|225|R50|I300|글로벌'\
            +'|10년|단기|금리|3년|배당|가치|은행|리츠|콜|TR'
    df.query(f'not itemname.str.contains("{kwds}")\
               or etfTabCode == 3', inplace=True)
    kwds2 = '레버리지|2X'
    df.query(f'not itemname.str.contains("{kwds2}")', inplace=True)
    df['category_marketSum_median'] = df['etfTabCode'].apply(
        lambda x: df[df.etfTabCode == x].marketSum.median())
    df['category_amonut_mean'] = df['etfTabCode'].apply(
        lambda x: df[df.etfTabCode == x].amonut.mean())
    kwds3 = 'BBIG|ESG|소프트웨어|테크|인터넷|플러스|MV|나스닥|스페이스|\sTOP10'\
            +'|HANARO|KOSEF|SOL|ARIRANG'
    df.query(f'not itemname.str.contains("{kwds3}")', inplace=True)
    expr = f'marketSum >= category_marketSum_median\
            and (amonut >= category_amonut_mean or etfTabCode == 7)\
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

if __name__ == '__main__':
    print(get_marketcap_from_naver())