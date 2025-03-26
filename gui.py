import tkinter as tk
from tkinter import scrolledtext
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging

class MyApp:
    def __init__(self, root, df_list, rate_vol,minAmount,barstart,barend,minPrice,min2pct,waitSeconds, api_instances):
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
        self.root.after(0, lambda: self.scroll_text.insert(tk.END, message + "\n"))
        logging.info(message)

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
        print('batch result:\n',  str_result)
        print(f'batch completed in: {(time.time() - batch_start):.2f} secs.')

        return str_result

    def process_single_stock(self, tdxdata, stock_code):
        df_min = tdxdata.get_minute_today(stock_code)
        # df_min = df_min[:177]
        if len(df_min)<self.barstart or len(df_min)>self.barend:
            return 0
        elif df_min['price'].values[-1]<self.minPrice:
            return 0
        else:
            df_min.reset_index(drop=False, inplace=True)
            df_min['index'] = df_min['index'].apply(lambda x: x + 1)
            df_min['pctr2'] = df_min['price'] / df_min['price'].shift(2)*100-100
            df_min['acc_vol'] = df_min['volume'].cumsum()
            df_min['avg_vol'] = df_min['acc_vol'] / df_min['index']
            df_min['rate_vol'] = df_min['volume'] / df_min['avg_vol'].shift(1)
            df_min['flt_price1'] = df_min['price'] > df_min['avg_price']
            df_min['flt_price'] = (df_min['flt_price1'] == True) & (df_min['flt_price1'].shift(2) == False)
            df_min['flt_volume'] = df_min['rate_vol'] > self.rate_vol
            df_min['flt_amount'] = df_min.apply(lambda x: x['volume']*x['price']>self.minAmount, axis=1)
            df_min['flt_pctr2'] = df_min.apply(lambda x: x['pctr2']>self.min2pct,axis=1)
            df_min['flag'] = (df_min['flt_price'] == True) & (df_min['flt_volume'] == True) & (df_min['flt_amount'] == True) & (df_min['flt_pctr2'] == True)

            if df_min['flag'].values[-1] == True:
                return round(df_min['price'].values[-1],3)
            else:
                return 0
