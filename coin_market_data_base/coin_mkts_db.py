from datetime import datetime, date, time
from poloniex_db import PoloMktDB
from dataset import connect

from multiprocessing import cpu_count

#from poloniex import Poloniex


if __name__ == "__main__":

    polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\',pairs='all')
     #['BTC_ETH', 'USDT_ETH', 'USDT_BTC', 'BTC_LSK', 'USDT_BCH', 'BTC_BCH', 'ETH_BCH', 'BTC_XMR'])

    #polodb.ScrapeHistoryData('all', datetime(2017, 8, 22, 8, 22, 0), datetime(2017, 8, 22, 8, 23, 00),\
    # period=480, local=False, write2db=True, process_num=6)

    polodb.AutoScrape(pairs='all', begin_datetime=datetime(2017, 8, 30, 0, 18, 1), local=True, \
     shutdown_datetime=datetime(2017, 8, 30, 0, 22, 0), period=240, process_num=6)

    print(list(polodb.All('BTC_ETH'))[0])
    print(list(polodb.All('BTC_ETH'))[-1])
