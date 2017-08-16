import os

from time import mktime
from datetime import datetime, timedelta
import pytz
from tzlocal import get_localzone

from sqlalchemy import create_engine, Column, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dataset import connect

from poloniex import Poloniex

class RelativeTime( object ):

    _platform_timezone = pytz.utc
    _local_timezone    = get_localzone()

    def __init__(self, pytzone=None):

        if pytzone:
            _platform_timezone = pytzone

    def ConverDateStrToTimeStamp(self, dates_str, local):

        unix_timestamps = []

        if local:
            for date_str in dates_str:
                date_datetime = datetime.strptime( date_str, "%Y-%m-%d %H:%M:%S")
                unix_timestamps.append(mktime(date_datetime.timetuple()))
        else:
            for date_str in dates_str:
                date_datetime = datetime.strptime( date_str, "%Y-%m-%d %H:%M:%S")
                date_datetime = date_datetime.replace(tzinfo=self._platform_timezone).astimezone(self._local_timezone)
                unix_timestamps.append(mktime(date_datetime.timetuple()))

        return unix_timestamps

    #def PlatformTime(self):
    #    return datetime.now() - timedelta(hours=self._platform_timezone)


class MktDBInfo( object ):

    _supportpairs  = []

    def __init__(self, platformname, platformobj, sqlcore, rootpath):
        self._sqlcore       = sqlcore
        self._rootpath      = rootpath
        self._platformname  = platformname
        self._dbpath        = sqlcore + ':///' + rootpath + 'poloniex.db'
        self._engine        = create_engine(self._dbpath, echo=False)
        self._platform      = platformobj

    def SetPlatform(self, platformobj):
        self._platform = platformobj

    def SqlEngine(self):
        return self._engine

    def SetSqlCore(self, sqlcore):
        self._sqlcore  = sqlcore

    def SetPlatformName(slef, platformname):
        self._platformname = platformname

    def SetRootPath(self, rootpath):
        self._rootpath = rootpath

    def SetSupportPairs(self, supportpairs):
        self._supportpairs = supportpairs[:]

Base = declarative_base()

class PoloMktDB( MktDBInfo, RelativeTime ):

    def __init__(self, sqlcore, rootpath):

        self.rawdata = []

        if not os.path.isdir(rootpath):
            os.makedirs(rootpath)

        MktDBInfo.__init__(self, "Poloniex", Poloniex(), sqlcore, rootpath)
        RelativeTime.__init__(self)
        self.__CreateMetaDataDB__()
        self.__ConnectWithDataSet__()

    def __CreateMetaDataDB__(self):
        Base.metadata.create_all(self._engine)

    def __ConnectWithDataSet__(self):
        self._db            = connect(self._dbpath)

    class PoloMktFormat( Base ):

        __tablename__ = "PoloniexHistoryTrade"
        id = Column(Integer, primary_key=True)
        tradeID   = Column(Integer)
        amount    = Column(Float(1E-8))
        rate      = Column(Float(1E-10))
        date      = Column(DateTime)
        buy_sell  = Column(Integer)

        def __repr__(self):
            return ("<PoloHistoryTrade>(tradeID='%s', category='%s', national='%s', gender:'%s', year:'%d')" \
            %(self.tradeID, self.amount, self.rate, self.date_time, self.buy_sell))

    # Saving to database or not.
    def SupportPairs(self, Saving=True):

        if Saving:
            self._supportpairs = self._platform.returnTicker().keys()

        return  self._platform.returnTicker().keys()

    def ShowSupportPairs(self):

        if len(self._supportpairs) != 0:
            print(self._supportpairs)
        else:
            print(self._platform.returnTicker().keys())

    def CleanRawData(self):

        # Pop globalTradeID and Total and rename 'type'
        for row in self.rawdata:
            [row.pop(delkeys, None) for delkeys in ['globalTradeID', 'total']]
            b_or_s = row.pop('type')[0]
            row['buy_sell'] = 1 if b_or_s == 'b' else -1
        # Convert type
        for row in self.rawdata:
            for key, value in row.items():
                if key[0] == 'd':
                    row[key] = datetime.strptime( row['date'], "%Y-%m-%d %H:%M:%S")
                elif key[0] in 'ar':
                    row[key] = float(value)

    def ScrapeHistoryData(self, pairs='All', datestr_end=None, datestr_begin=None, clean=False, local=True):

        if type(pairs) != type([]):
            pairs = [pairs]

        for pair in pairs:

            date_end, date_begin = self.ConverDateStrToTimeStamp([datestr_end, datestr_begin], local)
            self.rawdata = self._platform.marketTradeHist(pair, date_begin, date_end)
            for i in range(len(self.rawdata)):
                print(self.rawdata[i])
            exit()

        if clean:
            self.CleanRawData()

        print(self.rawdata[0]['date'])

if __name__ == "__main__":

    polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\')
    # So for we just support filling local time
    polodb.ScrapeHistoryData('BTC_ETH', "2017-08-16 06:52:00", "2017-08-16 06:50:00", clean=True, local=False)

    #testdata =[
    #{'globalTradeID': 210069439, 'tradeID': 1535855, 'date': '2017-08-16 02:54:49', 'type': 'buy', 'rate': '0.00000034', 'amount': '3600.67647059', 'total': '0.00122423'},
    #{'globalTradeID': 210069440, 'tradeID': 1535856, 'date': '2017-08-16 02:54:49', 'type': 'buy', 'rate': '0.00000034', 'amount': '3600.67647059', 'total': '0.00122423'},
    #{'globalTradeID': 210069441, 'tradeID': 1535857, 'date': '2017-08-16 02:54:49', 'type': 'sell', 'rate': '0.00000034', 'amount': '3600.67647059', 'total': '0.00122423'},
    #{'globalTradeID': 210069442, 'tradeID': 1535858, 'date': '2017-08-16 02:54:49', 'type': 'sell', 'rate': '0.00000034', 'amount': '3600.67647059', 'total': '0.00122423'},
    #{'globalTradeID': 210069443, 'tradeID': 1535859, 'date': '2017-08-16 02:54:49', 'type': 'buy', 'rate': '0.00000034', 'amount': '3600.67647059', 'total': '0.00122423'},
    #]
