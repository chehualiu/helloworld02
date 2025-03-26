import time
import pandas as pd
from datetime import datetime


def processStkBatches(self, tdxdata, batch):
    batch_start = time.time()
    current_time = datetime.now().strftime("%H:%M:%S")
    counter = 0
    batch_result = []
    for code, name in batch.items():
        rtn = self.process_single_stock(tdxdata, code)
        if rtn > 0:
            # self.root.after(0, self.update_result, code, name, current_time, rtn)
            batch_result.append(f"{current_time} {code} {name} {rtn:.2f}")

        counter += 1
        if counter % 100 == 0:
            print('.' + str(counter))
        else:
            print('.', end='', flush=True)

    str_result = "\n".join(batch_result)
    print('batch result:\n', str_result)
    print(f'batch completed in: {(time.time() - batch_start):.2f} secs.')

    return str_result


def process_single_stock(self, tdxdata, stock_code):
    df_min = tdxdata.get_minute_today(stock_code)
    # df_min = df_min[:177]
    if len(df_min) < self.barstart or len(df_min) > self.barend:
        return 0
    elif df_min['price'].values[-1] < self.minPrice:
        return 0
    else:
        df_min.reset_index(drop=False, inplace=True)
        df_min['index'] = df_min['index'].apply(lambda x: x + 1)
        df_min['pctr2'] = df_min['price'] / df_min['price'].shift(2) * 100 - 100
        df_min['acc_vol'] = df_min['volume'].cumsum()
        df_min['avg_vol'] = df_min['acc_vol'] / df_min['index']
        df_min['rate_vol'] = df_min['volume'] / df_min['avg_vol'].shift(1)
        df_min['flt_price1'] = df_min['price'] > df_min['avg_price']
        df_min['flt_price'] = (df_min['flt_price1'] == True) & (df_min['flt_price1'].shift(2) == False)
        df_min['flt_volume'] = df_min['rate_vol'] > self.rate_vol
        df_min['flt_amount'] = df_min.apply(lambda x: x['volume'] * x['price'] > self.minAmount, axis=1)
        df_min['flt_pctr2'] = df_min.apply(lambda x: x['pctr2'] > self.min2pct, axis=1)
        df_min['flag'] = (df_min['flt_price'] == True) & (df_min['flt_volume'] == True) & (
                    df_min['flt_amount'] == True) & (df_min['flt_pctr2'] == True)

        if df_min['flag'].values[-1] == True:
            return round(df_min['price'].values[-1], 3)
        else:
            return 0