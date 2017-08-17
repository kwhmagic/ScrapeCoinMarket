from datetime import datetime, date, time
from poloniex_db import PoloMktDB

if __name__ == "__main__":

    polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\')
    ## So for we just support filling local time
    #polodb.ScrapeHistoryData('BTC_ETH', "2017-08-16 06:52:00", "2017-08-16 06:50:00", clean=True, local=False)

    testdata =[
    {'pair': 'BTC_ETH', 'tradeID': 1535855, 'date': date(2017,8,16), 'time': time(2,54,49), 'buy_sell': 1 , 'rate': 0.00000034, 'amount': 3600.67647059},
    {'pair': 'BTC_ETH', 'tradeID': 1535856, 'date': date(2017,8,16), 'time': time(2,54,49), 'buy_sell': 1 , 'rate': 0.00000034, 'amount': 3600.67647059},
    {'pair': 'BTC_ETH', 'tradeID': 1535857, 'date': date(2017,8,16), 'time': time(2,54,49), 'buy_sell': -1, 'rate': 0.00000034, 'amount': 3600.67647059},
    {'pair': 'BTC_ETH', 'tradeID': 1535858, 'date': date(2017,8,16), 'time': time(2,54,49), 'buy_sell': -1, 'rate': 0.00000034, 'amount': 3600.67647059},
    {'pair': 'BTC_ETH', 'tradeID': 1535859, 'date': date(2017,8,16), 'time': time(2,54,49), 'buy_sell': 1 , 'rate': 0.00000034, 'amount': 3600.67647059},
    ]

    #polodb.Add_All(testdata)
    polodb.Delete(testdata[1])
    #polodb.Delete(testdata[2])
    ##polodb.Add(testdata[3])
    ##polodb.Add(testdata[4])
    #polodb.New()
    #polodb.Commit()
    print(polodb.All())
    #
    print(polodb.Count())
    #db = connect('sqlite:///C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\poloniex.db')
    #wtable = db['PoloniexHistoryTrade']
    #trades = wtable.find()

    #print(list(trades))
