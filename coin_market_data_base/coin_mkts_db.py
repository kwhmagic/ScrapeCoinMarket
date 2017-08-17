from datetime import datetime, date, time
from poloniex_db import PoloMktDB
from dataset import connect

if __name__ == "__main__":

    polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\')
    t1 = list(polodb.QueryDate(['BTC_ETH'], date(2017, 8, 16)))
    print(len(t1))
