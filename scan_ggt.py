import tkinter as tk
from tkinter import scrolledtext
from concurrent.futures import ThreadPoolExecutor
import requests
import time,warnings
import pandas as pd
from datetime import datetime
from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API
from tdx_hosts import hq_hosts, Exhq_hosts
import configparser
import logging
from tdx_hosts import hq_hosts, Exhq_hosts
from tdx_indicator import REF,SMA,MAX,ABS

warnings.filterwarnings("ignore")

class mytdxData(object):

    def __init__(self):

        Exapi = TdxExHq_API(heartbeat=True)
        exapilist = pd.DataFrame(Exhq_hosts)
        exapilist.columns = ['name', 'ip', 'port']
        if not self.TestConnection(Exapi, 'ExHQ', exapilist['ip'].values[0], exapilist['port'].values[0]):
            if not self.TestConnection(Exapi, 'ExHQ', exapilist['ip'].values[1], exapilist['port'].values[1]):
                if not self.TestConnection(Exapi, 'ExHQ', exapilist['ip'].values[2], exapilist['port'].values[2]):
                    if not self.TestConnection(Exapi, 'ExHQ', exapilist['ip'].values[3], exapilist['port'].values[3]):
                        print('All ExHQ server Failed!!!')
                    else:
                        print(f'connection to ExHQ server[3]!{exapilist["name"].values[3]}')
                else:
                    print(f'connection to ExHQ server[2]!{exapilist["name"].values[2]}')
            else:
                print(f'connection to ExHQ server[1]!{exapilist["name"].values[1]}')
        else:
            print(f'connection to ExHQ server[0]!{exapilist["name"].values[0]}')

        self.Exapi = Exapi

    def TestConnection(self, Api, type, ip, port):
        if type == 'HQ':
            try:
                is_connect = Api.connect(ip, port)
            except Exception as e:
                print(f'connect to HQ Exception: {e}')
                logging.error(f'connect to {type} Exception: {e}')
                return False
            return is_connect
        elif type == 'ExHQ':
            try:
                is_connect = Api.connect(ip, port)
            except Exception as e:
                print(f'connect to ExHQ Exception: {e}')
                return False
            return is_connect

    def get_market_code(self, code):

        mkt = None
        fuquan = False
        isIndex = False

        if '#' in code:
            mkt = int(code.split('#')[0])
            code = code.split('#')[1]

        if code[:2] == 'ZS':
            code = code[-6:]
            isIndex = True
            fuquan = False

        if code.isdigit() and len(code) == 6:  # A股
            if isIndex == True:
                mkt = 1 if code[:2] == '00' else 0  # 上证指数，深证成指
            elif code[:2] in ['15', '00', '30', '16', '12', '39', '18']:  # 深市
                mkt = 0  # 深交所
                fuquan = True
            elif code[:2] in ['51', '58', '56', '60', '68', '50', '88', '11', '99']:
                mkt = 1  # 上交所
                fuquan = True
            elif code[:2] in ['43', '83', '87']:
                mkt = 2  # 北交所pass
                fuquan = True
            else:
                pass

            if code[:2] in ['39', '88', '99']:
                isIndex = True
                fuquan = False

        elif code.isdigit() and len(code) == 5:  # 港股
            mkt = 71
            fuquan = True
        elif code.isdigit() and len(code) == 8:  # 期权
            if code[:2] == '10':
                mkt = 8
            elif code[:2] == '90':
                mkt = 9
            else:
                mkt = None
        elif code.isalpha():  # 美股
            mkt = None
        elif len(code) == 5 and code[0] == 'U':  # 期权指标如持仓比
            mkt = 68
        else:
            mkt = None
        return mkt, code, fuquan, isIndex

    def get_minute_today(self, code):
        mkt, code, fuquan, isIndex = self.get_market_code(code)

        if mkt in [71] and isIndex == False:
            data = pd.DataFrame(self.Exapi.get_minute_time_data(mkt, code))
        else:
            data = pd.DataFrame()
        return data


def initialize_api(N):
    instances = []
    for i in range(N):
        instance = mytdxData()
        instances.append(instance)
    return instances


class MyApp:
    def __init__(self, root, df_list, rate_vol,minAmount,barstart,barend,minPrice,RSIN,RSILow,RSIHigh,
                 min2pct,waitSeconds, api_instances):
        self.root = root
        self.root.title("港股通扫描程序")
        self.batchCnt = len(api_instances)
        self.api_instances = api_instances
        self.df_list = df_list
        self.min2pct = min2pct
        self.waitSeconds = waitSeconds
        self.rate_vol = rate_vol
        self.barstart = barstart
        self.barend = barend
        self.minPrice = minPrice
        self.RSIN = RSIN
        self.RSILow = RSILow
        self.RSIHigh = RSIHigh
        self.minAmount = minAmount

        stockPerBatch = len(df_list) // self.batchCnt
        stockBatchList = []

        for i in range(self.batchCnt):
            if i < self.batchCnt - 1:
                sl_temp = df_list[i * stockPerBatch: (i + 1) * stockPerBatch]
            else:
                sl_temp = df_list[i * stockPerBatch:]
            stocklist_temp = {}
            for index, row in sl_temp.iterrows():
                stocklist_temp[row['code']] = row['name']
            stockBatchList.append(stocklist_temp)

        self.stockBatchList = stockBatchList

        # Create a scrollable text area to display results
        self.scroll_text = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=60, height=20)
        self.scroll_text.pack(padx=10, pady=10)

        # Create a frame for buttons
        button_frame = tk.Frame(root)
        button_frame.pack(pady=10)

        # Start button
        self.start_button = tk.Button(button_frame, text="开始扫描", command=self.start_repeated_tasks)
        self.start_button.pack(side=tk.LEFT, padx=5)

        # Stop button
        self.stop_button = tk.Button(button_frame, text="停止扫描", command=self.stop_repeated_tasks, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        # Exit button
        self.exit_button = tk.Button(button_frame, text="退出程序", command=self.exit_program)
        self.exit_button.pack(side=tk.LEFT, padx=5)

        self.is_running = False  # Flag to control the task loop

    def start_repeated_tasks(self):
        if not self.is_running:
            self.is_running = True
            self.start_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
            self.root.after(0, self.update_event, f'===== {datetime.now().strftime("%H:%M:%S")} 开始扫描,间隔{self.waitSeconds}秒')
            self.start_batches()

    def stop_repeated_tasks(self):
        if self.is_running:
            self.is_running = False
            self.root.after(0, self.update_event, f'===== {datetime.now().strftime("%H:%M:%S")} 已停止扫描')
            self.start_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)

    def exit_program(self):
        self.root.destroy()
    def start_batches(self):
        # Use ThreadPoolExecutor to run tasks concurrently
        with ThreadPoolExecutor(max_workers=self.batchCnt) as executor:
            futures = [executor.submit(self.processStkBatches, api, batch) for api, batch in zip(self.api_instances, self.stockBatchList)]

            for future in futures:
                try:
                    result = future.result()  # This will block until the task is complete
                    if len(result) > 10:
                        self.update_text_area(result)
                except Exception as e:
                    self.update_text_area(f"错误: {e}")

        # self.root.after(0, self.update_event, f'===== {self.waitSeconds}秒后开始下一轮扫描')
        if self.is_running:
            self.root.after(self.waitSeconds * 1000, self.start_batches)


    def update_text_area(self, message):
        # Update the text area in the main thread
        # self.root.after(0, lambda: self.scroll_text.insert(tk.END, message + "\n"))
        message = '------------------\n' + message
        self.root.after(0, lambda: self.update_event(message))
        # logging.info(message)

    def update_result(self, stock_code, stock_name, current_time, price):
        # Display stock information in the GUI
        message = f" {current_time} {stock_code} {stock_name}  @ {price:.2f}\n"
        self.scroll_text.insert(tk.END, message)
        self.scroll_text.see(tk.END)  # Automatically scroll to the latest message
        logging.info(message)

    def update_event(self, msg):
        message = f"{msg}\n"
        self.scroll_text.insert(tk.END, message)
        self.scroll_text.see(tk.END)  # Automatically scroll to the latest message
        logging.info(msg)

    def processStkBatches(self, tdxdata, batch):
        batch_start = time.time()
        current_time = datetime.now().strftime("%H:%M:%S")
        counter = 0
        batch_result = []
        for code, name in batch.items():
            reason,rtn = self.process_single_stock(tdxdata, code)
            if rtn > 0:
                # self.root.after(0, self.update_result, code, name, current_time, rtn)
                batch_result.append(f"{current_time} {reason} {code} {name} {rtn:.2f}")

            counter += 1
            if counter % 100 == 0:
                print('.' + str(counter))
            else:
                print('.', end='', flush=True)

        str_result = "\n".join(batch_result)
        # print('batch result:\n',  str_result)
        print(f'{current_time} batch completed in: {(time.time() - batch_start):.2f} secs.')

        return str_result

    def process_single_stock(self, tdxdata, stock_code):
        df_min = tdxdata.get_minute_today(stock_code)
        # df_min = df_min[:177]
        if len(df_min)<self.barstart or len(df_min)>self.barend:
            return '',0
        elif df_min['price'].values[-1]<self.minPrice:
            return '',0
        else:
            df_min['amt'] = df_min['price'] * df_min['volume']
            if df_min['amt'].mean() < minAmount:
                return '',0
            df_min.reset_index(drop=False, inplace=True)
            df_min['m10pct'] = df_min['price']/df_min['price'].shift(10)-1
            df_min['m10drop'] = df_min['m10pct'] < -0.01
            df_min['m10drop'] = df_min['m10drop'].map({True: 1, False: 0})
            df_min['m10dropflag'] = df_min['m10drop'].rolling(window=3).sum()
            close = df_min['price']
            r1 = REF(close, 1)
            RSI = SMA(MAX(close - r1, 0), self.RSIN, 1) / SMA(ABS(close - r1), self.RSIN, 1)
            df_min['RSI'] = RSI
            df_min['lowupsig'] = (df_min['RSI'].shift(1) < 0.2) & (df_min['RSI'] > df_min['RSI'].shift(1)) & (df_min['m10dropflag'] > 0)

            df_min['cummax'] = df_min['price'].cummax()
            df_min['cummin'] = df_min['price'].cummin()
            df_min['higsig'] = ((df_min['price'] == df_min['cummax']) & (df_min['price'] > df_min['price'].shift(1))).map({True: 1, False: 0})
            df_min['lowsig'] = ((df_min['price'] == df_min['cummin']) & (df_min['price'] < df_min['price'].shift(1))).map({True: 1, False: 0})
            higgroup = (df_min['higsig'] != df_min['higsig'].shift()).cumsum()
            df_min['higgroup'] = higgroup
            df_min['higcons'] = df_min.groupby('higgroup').cumcount() + 1
            df_min['higcons_cnt'] = df_min.apply(lambda x: x['higcons'] if x['higsig'] == 1 else 0, axis=1)
            lowgroup = (df_min['lowsig'] != df_min['lowsig'].shift()).cumsum()
            df_min['lowgroup'] = lowgroup
            df_min['lowcons'] = df_min.groupby('lowgroup').cumcount() + 1
            df_min['lowcons_cnt'] = df_min.apply(lambda x: x['lowcons'] if x['lowsig'] == 1 else 0, axis=1)

            if df_min['lowupsig'].values[-1] == True:
                return '低位回升',round(df_min['price'].values[-1], 3)
            elif df_min['higcons_cnt'].values[-1] > 1:
                return '新高',round(df_min['price'].values[-1], 3)
            else:
                return '',0


# Main program
if __name__ == "__main__":

    cfg_fn = 'scan_ggt.cfg'
    config = configparser.ConfigParser()
    config.read(cfg_fn, encoding='utf-8')
    NumThreads = int(dict(config.items('params'))['numthreads'])  
    RSIN = int(dict(config.items('params'))['rsin'])
    barstart = int(dict(config.items('params'))['barstart'])
    barend = int(dict(config.items('params'))['barend'])
    fn_stocklist = dict(config.items('params'))['stock_path']
    min2pct = float(dict(config.items('params'))['min2pct'])
    rate_vol = float(dict(config.items('params'))['rate_vol'])
    minPrice = float(dict(config.items('params'))['minprice'])
    minAmount = float(dict(config.items('params'))['minamount'])
    RSILow = float(dict(config.items('params'))['rsilow'])
    RSIHigh = float(dict(config.items('params'))['rsihigh'])
    waitSeconds = int(dict(config.items('params'))['timegap'])
    instances = initialize_api(NumThreads)

    log_filename = f'logs\\scan_ggt_{datetime.now().strftime("%y%m%d")}.log'
    logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(message)s')

    df_list = pd.read_csv(fn_stocklist, encoding='gbk', dtype={'code': str})
    df_list = df_list[df_list['flag'] == 'Y']
    # df_list = df_list[:5]

    print(f'''共{len(df_list)}只股票待扫描''')

    root = tk.Tk()
    app = MyApp(root, df_list=df_list, rate_vol=rate_vol, minAmount=minAmount,
                barstart=barstart, barend=barend, minPrice=minPrice,RSIN=RSIN,
                RSILow = RSILow, RSIHigh = RSIHigh,
                min2pct=min2pct, waitSeconds=waitSeconds, api_instances=instances)
    root.mainloop()



