from datetime import datetime, date, time
from poloniex_db import PoloMktDB
from coin_db_manage import CoinDBManage

if __name__ == "__main__":

    #coindb = CoinDBManage('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\', platformname='all', pairs='all')
    #USDT_BCH_Poloniex = coindb.PairDataFromXPlatform_All('USDT_BCH', 'Poloniex', convert2dataframe=True)
    #print(USDT_BCH_Poloniex)

    polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\',pairs=['USDT_BCH'])
    #polodb.AutoScrape(pairs='all', begin_datetime=datetime(2017, 8, 31, 3, 21, 1), local=True, \
    # shutdown_datetime=datetime(2017, 8, 31, 5, 34, 0), period=120, process_num=1)
    #polodb.ScrapeHistoryData('all', datetime(2017, 8, 22, 8, 22, 0), datetime(2017, 8, 22, 9, 23, 00),\
    # period=30, local=False, write2db=False, clean=True, process_num=1)

    db_usdt_bch = polodb.All('USDT_BCH')
    print(db_usdt_bch[0])
    print(db_usdt_bch[-1])

    #startID = db_usdt_bch[0].tradeID
    #for row in db_usdt_bch[1:]:
    #    if row.tradeID != (startID+1):
    #        print("QQ tradeID(%d, %d)" %(startID, row['tradeID']))
    #        break
    #    else:
    #        print("True")
    #    startID += 1
