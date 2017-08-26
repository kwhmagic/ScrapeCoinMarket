from datetime import datetime, date, time
from poloniex_db import PoloMktDB
from dataset import connect

from multiprocessing import cpu_count

#from poloniex import Poloniex


if __name__ == "__main__":

    polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\', ['BTC_ETH', 'USDT_ETH', 'USDT_BTC'])
    #polodb.AutoScrape(pairs=['BTC_ETH', 'USDT_ETH'], begin_datetime='2017-08-15 08:22:00', process_num=2)
    t = [{"tradeID": 1, "amount": 12, "rate": 0.1, "date": date(2017, 8, 22), "time": time(8, 22, 0), "buy_sell": 1},
    {"tradeID": 2, "amount": 13, "rate": 0.2, "date": date(2017, 8, 22), "time": time(8, 22, 0), "buy_sell": 1},
    {"tradeID": 3, "amount": 14, "rate": 0.3, "date": date(2017, 8, 22), "time": time(8, 22, 0), "buy_sell": 1}
    ]

    polodb.Add_All('BTC_ETH', t, reverse=True)
    polodb.Commit('BTC_ETH')
    print(polodb.Count('BTC_ETH'))
    print(polodb.Count('USDT_ETH'))
    print(polodb.All('BTC_ETH'))
