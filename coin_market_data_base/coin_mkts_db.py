from poloniex_db import PoloMktDB
from coin_db_manage import CoinDBManage

if __name__ == "__main__":

    #polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\',pairs='all')
    #polodb.AutoScrape(pairs='all', begin_datetime=datetime(2017, 8, 30, 18, 32, 1), local=True, \
    # shutdown_datetime=datetime(2017, 8, 30, 18, 39, 0), period=240, process_num=6)
    #polodb.ScrapeHistoryData('all', datetime(2017, 8, 22, 8, 22, 0), datetime(2017, 8, 22, 8, 23, 00),\
    # period=480, local=False, write2db=True, process_num=6)

    coindb = CoinDBManage('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\', platformname='all', pairs='all')
    USDT_BCH_Poloniex = coindb.PairDataFromXPlatform_All('USDT_BCH', 'Poloniex', convert2dataframe=True)
    print(USDT_BCH_Poloniex)
