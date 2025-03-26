from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API
from tdx_hosts import hq_hosts, Exhq_hosts
import pandas as pd

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
