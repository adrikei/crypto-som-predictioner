import psycopg2
import pandas as pd
import numpy as np
import talib

succeeded_connecting = False
try:
    conn = psycopg2.connect("dbname='adriano' user='adriano' host='localhost' password='adriano'")
    succeeded_connecting = True
except:
    print("I am unable to connect to the database")

if succeeded_connecting:
    cur = conn.cursor()

    cur.execute("""select 
                    ts,
                    extract('month' from ts) as month,
                    extract('day' from ts) as day,
                    extract('hour' from ts) as hour,
                    extract('minute' from ts) as minute,
                    extract('second' from ts) as second,
                    buy_trades,
                    buy_sum, 
                    buy_avg_w_price, 
                    sell_trades, 
                    sell_sum, 
                    sell_avg_w_price, 
                    wavg
                from bu_sa_t10 order by ts""")

    rows = cur.fetchall()

    initial_cols = ['ts', 'month', 'day', 'hour', 'minute', 'second', 'buy_trades', 'buy_sum', 'buy_wavg', 'sell_trades', 'sell_sum', 'sell_wavg', 'wavg']

    df = pd.DataFrame(rows)
    df.columns = initial_cols

    del rows

    group_min1 = df[['month','day','hour','minute','wavg']].groupby(['month','day','hour','minute']).agg('last')
    min1_close = np.array(group_min1['wavg'])
    group_min1['ema13'] = talib.EMA(min1_close, timeperiod=13) #ema13
    group_min1['ema21'] = talib.EMA(min1_close, timeperiod=21) #ema21
    group_min1['ema55'] = talib.EMA(min1_close, timeperiod=55) #ema55
    group_min1['rsi2'] = talib.RSI(min1_close, timeperiod=2) #rsi2
    group_min1['rsi14'] = talib.RSI(min1_close, timeperiod=14) #rsi14
    group_min1['macd_macd'], group_min1['macd_signal'], group_min1['macd_hist'] = talib.MACD(min1_close)

    min1_low = np.array(df[['month','day','hour','minute','wavg']].groupby(['month','day','hour','minute']).agg('min')['wavg'])
    min1_high = np.array(df[['month','day','hour','minute','wavg']].groupby(['month','day','hour','minute']).agg('max')['wavg'])

    group_min1['ultosc'] = talib.ULTOSC(min1_high, min1_low, min1_close) #ultimate oscilator - 0 to 100
    group_min1['willr'] = talib.WILLR(min1_high, min1_low, min1_close) #williams %r - 0 to -100
    
    group_min1_others = df[['month','day','hour','minute','buy_trades','buy_sum', 'sell_trades', 'sell_sum']].groupby(['month','day','hour','minute']).agg('sum')
    group_min1_others['total_sum'] = group_min1_others['buy_sum'] + group_min1_others['sell_sum']
    
    min1_n_buys = np.array(group_min1_others['buy_trades'])
    min1_v_buys = np.array(group_min1_others['buy_sum'])
    min1_n_sells = np.array(group_min1_others['sell_trades'])
    min1_v_sells = np.array(group_min1_others['sell_sum'])
    
    group_min1_others['obv'] = talib.OBV(min1_close, np.array(group_min1_others['total_sum']))
    group_min1_others.drop(columns='total_sum', inplace=True)   
    
    # https://www.quantopian.com/posts/preprocess-slash-normalize-stock-prices-as-an-input-to-a-statistic-analysis
    # https://www.youtube.com/watch?v=PtoUlt3V0CI
    def log_return_array(arr):
        new = []
        for i in range(len(arr)-1):
            idx = i+1
            # does this even make sense?
            dividend = arr[idx]
            divisor = arr[i]
            flag = 1
            if (dividend > 0 and divisor < 0) or (dividend < 0 and divisor > 0):
                flag = -1
                dividend = dividend * (-1)
            
            new.append(flag * np.log(dividend/divisor))
        new.insert(0, np.nan)
        return new
    
    group_min1['wavg_lr'] = log_return_array(group_min1['wavg'].values)
    group_min1['ema13'] = log_return_array(group_min1['ema13'].values)
    group_min1['ema21'] = log_return_array(group_min1['ema21'].values)
    group_min1['ema55'] = log_return_array(group_min1['ema55'].values)
    group_min1['macd_macd'] = log_return_array(group_min1['macd_macd'].values)
    group_min1['macd_signal']  = log_return_array(group_min1['macd_signal'].values)
    group_min1['macd_hist'] = log_return_array(group_min1['macd_hist'].values)
    
    group_min1_others['obv'] = log_return_array(group_min1_others['obv'].values)
    
    # RSI/willr/ultimate oscilator can be divided by 100
    group_min1['rsi2'] = np.divide(group_min1['rsi2'].values, 100)
    group_min1['rsi14'] = np.divide(group_min1['rsi14'].values, 100)
    group_min1['ultosc'] = np.divide(group_min1['ultosc'].values, 100)
    group_min1['willr'] = np.divide(group_min1['willr'].values, 100)
    
    df = df.set_index(['month','day','hour','minute']).join(group_min1.drop(columns=['wavg']))
    df = df.join(group_min1_others[['obv']])
    
#    df['buy_trades_lr'] = log_return_array(df['buy_trades'].values)
#    df['sell_trades_lr'] = log_return_array(df['sell_trades'].values)
    df['wavg_lr'] = log_return_array(df['wavg'].values)
    
    new_cols = [v for v in [col if col not in initial_cols else None for col in df.columns] if v is not None]
    
    del group_min1
    del group_min1_others
    del min1_close

    for col in new_cols:
        query = 'alter table bu_sa_t10 add column if not exists %s float;' % (col)
        cur.execute(query)

    # drop nulls so it doesn't update in case we end up needing to do multiple runs to work on all data

    query = 'update bu_sa_t10 set %s' % (', '.join([col+' = %.32f' for col in new_cols])) + ' where ts = \'%s\';'
    
    iterating = df.dropna().iterrows()
    for row in iterating:
        update = query % (row[1]['ema13'],row[1]['ema21'],row[1]['ema55'],
                          row[1]['rsi2'],row[1]['rsi14'],row[1]['macd_macd'],
                          row[1]['macd_signal'],row[1]['macd_hist'],row[1]['ultosc'],
                          row[1]['willr'],row[1]['obv'],
#                          row[1]['buy_trades_lr'],
#                          row[1]['sell_trades_lr'],
                          row[1]['wavg_lr'],row[1]['ts'])
        query = query.replace('nan', 'null')
        print('Updating %s' %(row[1][0]))
        cur.execute(update)
        
    conn.commit()
    cur.close()
    conn.close()

del df
