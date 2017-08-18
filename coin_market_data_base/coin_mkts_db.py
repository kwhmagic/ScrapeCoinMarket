from datetime import datetime, date, time
from poloniex_db import PoloMktDB
from dataset import connect
#import parallelTestModule


if __name__ == "__main__":

    #extractor = parallelTestModule.ParallelExtractor()
    #extractor.runInParallel(numProcesses=2, numThreads=4)

    polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\')
    polodb.AutoScrape(pairs=['BTC_ETH', 'USDT_REP', 'USDT_DASH', 'USDT_ETC'], begin_datetime='2017-08-15 08:22:00', threadsnum=1)
    #polodb.ScrapeHistoryData(['BTC_ETH', 'BTC_LTC', 'BTC_LSK', 'USDT_ETH'],\
    # "2017-08-16 08:22:00",  "2017-08-14 08:22:00", local=False, write2db=True)
    #dataOn20170816 = polodb.QueryDate(['BTC_LSK'], date(2017, 8, 15), date(2017, 8, 16))
    #print(dataOn20170816)
    #print(len(dataOn20170816))
    #dataETHOn20170816 = list(polodb.QueryDate(['BTC_ETH'], date(2017, 8, 16)))
    #print(len(dataETHOn20170816))
