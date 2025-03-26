import configparser
import logging
from datetime import datetime
import pandas as pd
import tkinter as tk
from gui import MyApp
from api import initialize_api
from utils import read_config

if __name__ == "__main__":
    config = read_config('scan_ggt.cfg')
    NumThreads = config['params']['numthreads']
    barstart = config['params']['barstart']
    barend = config['params']['barend']
    fn_stocklist = config['params']['stock_path']
    min2pct = config['params']['min2pct']
    rate_vol = config['params']['rate_vol']
    minPrice = config['params']['minprice']
    minAmount = config['params']['minamount']
    waitSeconds = config['params']['timegap']
    instances = initialize_api(NumThreads)

    log_filename = f'logs\\scan_ggt_{datetime.now().strftime("%y%m%d")}.log'
    logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(message)s')

    df_list = pd.read_csv(fn_stocklist, encoding='gbk', dtype={'code': str})
    df_list = df_list[df_list['flag'] == 'Y']

    print(f'''共{len(df_list)}只股票待扫描''')

    root = tk.Tk()
    app = MyApp(root, df_list=df_list, rate_vol=rate_vol, minAmount=minAmount,
                barstart=barstart, barend=barend, minPrice=minPrice,
                min2pct=min2pct, waitSeconds=waitSeconds, api_instances=instances)
    root.mainloop()
