import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine, VARCHAR
from pangres import upsert
import hashlib
import tushare as ts
import pandas as pd
from datetime import datetime
import time
from crawlab import save_item

pro = ts.pro_api('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

#填写mysql的相关信息，地址、table、密码
engine_ts = create_engine('mysql://数据库:密码@地址:3306/table')

today = datetime.now().strftime('%Y%m%d')

def hash(sourcedf,destinationdf,*column):
    columnName = ''
    destinationdf['hash_'+columnName.join(column)] = pd.DataFrame(sourcedf[list(column)].values.sum(axis=1))[0].str.encode('utf-8').apply(lambda x: (hashlib.sha512(x).hexdigest().upper()))


def daily_basic(ts_code, start_date, end_date, name, industry, list_date):
    df = pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)
    df['ts_type'] = 'daily_basic'
    df['name'] = name
    df['industry'] = industry
    df['list_date'] = list_date
    df['id'] = df['ts_code'] + '-' + df['trade_date'] + '-' + df['close'].astype(str) + '-' + df['ts_type']
    hash(df, df, 'id', 'id')
    df = df.drop_duplicates(subset=['hash_idid'], keep='last')
    df = df.set_index('hash_idid')
    #print(df.to_markdown())
    return df

dtype = {'hash_idid':VARCHAR(128)}

#主要用于获取代码对应的中文名称
stock_basic = pro.stock_basic(exchange='', list_status='L')
stock_basic.set_index('symbol', inplace=True)
stock_basic = stock_basic[stock_basic["ts_code"].str.startswith('688')]


for ts_code, name, industry, list_date in zip(stock_basic['ts_code'], stock_basic['name'], stock_basic['industry'], stock_basic['list_date']):
    print(ts_code)

    try:
        basics = daily_basic(ts_code, list_date, today, name, industry, list_date)

        upsert(engine=engine_ts,
               df=basics,
               table_name='kcb_data',
               if_row_exists='update',
               dtype=dtype)
    except Exception as e:
        print(e)

    time.sleep(6)