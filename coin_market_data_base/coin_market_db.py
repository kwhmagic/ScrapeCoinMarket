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

Base = declarative_base()

class MktDBInfo( object ):

    _supportpairs  = []

    def __init__(self, platformname, platformobj, sqlcore, rootpath):
        self._sqlcore       = sqlcore
        self._rootpath      = rootpath
        self._platformname  = platformname
        self._dbpath        = sqlcore + ':///' + rootpath + 'poloniex.db'
        self._engine        = create_engine(self._dbpath, echo=False)
        self._platform      = platformobj
        Base.metadata.create_all(self._engine)
        self._db            = connect(self._dbpath)
        Session = sessionmaker(bind=self._engine)
        self._session = Session()

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

    def __Add__(self, mktobj):
        self._session.add(mktobj)

    def __New__(self):
        self._session.new

    def __Commit__(self):
        self._session.commit

    def __Count__(self, mktobj):
        return self._session.query(mktobj).count()

class PoloMktDB( MktDBInfo, RelativeTime ):

    class PoloMktFormat( Base ):

        __tablename__ = "PoloniexHistoryTrade"
        id = Column(Integer, primary_key=True)
        tradeID   = Column(Integer)
        amount    = Column(Float(1E-8))
        rate      = Column(Float(1E-10))
        date      = Column(DateTime)
        buy_sell  = Column(Integer)

        def __repr__(self):
            return ("<PoloHistoryTrade>(tradeID='%d', amount='%.8f', rate='%.10f', date:'%s', buyOrSell:'%d')" \
            %(self.tradeID, self.amount, self.rate, self.date, self.buy_sell))

    def __init__(self, sqlcore, rootpath):

        self.rawdata = []

        if not os.path.isdir(rootpath):
            os.makedirs(rootpath)

        MktDBInfo.__init__(self, "Poloniex", Poloniex(), sqlcore, rootpath)
        RelativeTime.__init__(self)

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

        if clean:
            self.CleanRawData()

    def Add(self, item=None):
        self.__Add__(self.PoloMktFormat(**item))

    def New(self):
        self.__New__()

    def All(self):
        return list(self._session.query(self.PoloMktFormat).all())

    def Count(self):
        return self.__Count__(self.PoloMktFormat)

    def Commit(self):
        self.__Commit__()


if __name__ == "__main__":

    polodb = PoloMktDB('sqlite', 'C:\\Users\\user\\GitRepo\\ScrapeCoinMarket\\DB\\')
    # So for we just support filling local time
    #polodb.ScrapeHistoryData('BTC_ETH', "2017-08-16 06:52:00", "2017-08-16 06:50:00", clean=True, local=False)

    polodb.Add(testdata[0])
    polodb.Add(testdata[1])
    polodb.New()
    print(polodb.Count())
    print(polodb.All())

    #testdata =[
    #{'tradeID': 1535855, 'date': datetime(2017,8,16,2,54,49), 'buy_sell': 1 , 'rate': 0.00000034, 'amount': 3600.67647059},
    #{'tradeID': 1535856, 'date': datetime(2017,8,16,2,54,49), 'buy_sell': 1 , 'rate': 0.00000034, 'amount': 3600.67647059},
    #{'tradeID': 1535857, 'date': datetime(2017,8,16,2,54,49), 'buy_sell': -1, 'rate': 0.00000034, 'amount': 3600.67647059},
    #{'tradeID': 1535858, 'date': datetime(2017,8,16,2,54,49), 'buy_sell': -1, 'rate': 0.00000034, 'amount': 3600.67647059},
    #{'tradeID': 1535859, 'date': datetime(2017,8,16,2,54,49), 'buy_sell': 1 , 'rate': 0.00000034, 'amount': 3600.67647059},
    #]
