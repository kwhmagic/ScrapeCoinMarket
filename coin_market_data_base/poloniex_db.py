import os
from datetime import datetime, date, time

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Date, Time, Float, Integer, String
from dataset import connect

from poloniex import Poloniex
from relativetime import RelativeTime
from mktdbinfo import MktDBInfo

Base      = declarative_base()

class PoloMktDB( MktDBInfo, RelativeTime ):

    class PoloMktFormat( Base ):

        __tablename__ = "PoloniexHistoryTrade"
        id = Column(Integer, primary_key=True)
        pair          = Column(String(50))
        tradeID       = Column(Integer)
        amount        = Column(Float(1E-8))
        rate          = Column(Float(1E-10))
        date          = Column(Date)
        time          = Column(Time)
        buy_sell      = Column(Integer)

        def __repr__(self):
            return ("<PoloHistoryTrade>(pair:'%s', tradeID:'%d', amount:'%.8f', rate:'%.10f', date:'%s', time:'%s', buyOrSell:'%d')\n" \
            %(self.pair, self.tradeID, self.amount, self.rate, self.date, self.time, self.buy_sell))

    def __init__(self, sqlcore, rootpath):

        self.cleandata, self.rawdata = [], []

        if not os.path.isdir(rootpath):
            os.makedirs(rootpath)

        MktDBInfo.__init__(self, "Poloniex", Poloniex(), self.PoloMktFormat, sqlcore, rootpath, Base)
        RelativeTime.__init__(self)

    def SupportPairs(self, Saving=True):

        if Saving:
            self._supportpairs = self._platform.returnTicker().keys()

        return  self._platform.returnTicker().keys()

    def ShowSupportPairs(self):

        if len(self._supportpairs) != 0:
            print(self._supportpairs)
        else:
            print(self._platform.returnTicker().keys())

    def RawData(self):
        return self.rawdata

    def CleanData(self):

        if len(self.cleandata) == 0 and len(self.rawdata) != 0:
            self.CleanRawData(delrawdata=False)

        return self.cleandata

    def ScrapeHistoryData(self, pairs='All', datestr_end=None, datestr_begin=None, local=True, write2db=False, rmrawdate=True):

        if type(pairs) != type([]):
            pairs = [pairs]

        for pair in pairs:

            date_end, date_begin = self.ConverDateStrToTimeStamp([datestr_end, datestr_begin], local)
            self.subdata = self._platform.marketTradeHist(pair, date_begin, date_end).copy()
            for row in self.subdata:
                row['pair'] = pair
            self.rawdata += self.subdata

            if write2db:
                self.CleanRawData(write2db, rmrawdate)

    def CleanRawData(self, write2db=False, delrawdata=True):

        cleanrow = dict()
        for row in self.rawdata:
            b_or_s = row['type'][0]
            cleanrow['buy_sell'] = 1 if b_or_s == 'b' else -1
            for key, value in row.items():
                if key[0] == 'd':
                    datestr, timestr = value.split(' ')
                    cleanrow['date'] = datetime.strptime( datestr, "%Y-%m-%d").date()
                    cleanrow['time'] = datetime.strptime( timestr, "%H:%M:%S").time()
                elif key[0] in 'ar':
                    cleanrow[key] = float(value)
                elif key == 'tradeID' or key == 'pair':
                    cleanrow[key] = value
            self.cleandata.append(cleanrow.copy())

        if delrawdata:
            self.rawdata = []

        if write2db:

            self.Add_All(self.cleandata)
            self.New()
            self.Commit()
            self.cleandata = []

    def Add(self, item):
        self.__Add__(self.PoloMktFormat(**item))

    def Add_All(self, itmes):

        mktobjs = []
        for item in itmes:
            mktobjs.append(self.PoloMktFormat(**item))

        self.__Add__All__(mktobjs)

    def Delete(self, item):
        self.__Delete__(self.PoloMktFormat(**item))

    def New(self):
        self.__New__()

    def Commit(self):
        self.__Commit__()

    def All(self):
        return list(self.__All__())

    def QueryDate(self, pairs='all', date_begin=None, date_end=None):

        data = []
        if pairs != 'all':
            pairs = [pairs] if type(pairs) != type([]) else pairs
        else:
            pairs = self.SupportPairs()

        if date_begin != None and date_end == None:
            for pair in pairs:
                data += self._query.filter(self.PoloMktFormat.pair == pair, self.PoloMktFormat.date == date_begin)

        elif date_begin != None and date_end != None:
            for pair in pairs:
                data += self._query.filter(self.PoloMktFormat.pair == pair,\
                self.PoloMktFormat.date >= date_begin, self.PoloMktFormat.date < date_end)

        elif date_begin == None and date_end == None:
            for pair in pairs:
                data += self._query.filter(self.PoloMktFormat.pair == pair)

        return data

    def Count(self):
        return self.__Count__()
