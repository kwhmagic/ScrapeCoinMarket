import os
import pprint
from datetime import datetime, date, time

from multiprocessing import Process, Queue

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


    def __AutoScrape_Driver__(self, queue, threadidx, pairs, begin_timestamp, end_timestamp):

        histdata_per_thread = []

        for pair in pairs:

            segbegin_timestamp, segend_timestamp = begin_timestamp, end_timestamp

            while True:

                segrange = segend_timestamp - segbegin_timestamp
                histdata_seg = self._platform.marketTradeHist(pair, segbegin_timestamp, segend_timestamp)

                if len(histdata_seg) >= 50000:
                    segrange /= 2
                    segend_timestamp = segbegin_timestamp + segrange
                else:
                    for row in histdata_seg:
                        row['pair'] = pair
                    histdata_per_thread += histdata_seg

                    if int(segend_timestamp) >= int(end_timestamp):
                        break

                    segbegin_timestamp, segend_timestamp = segend_timestamp, end_timestamp

        return (histdata_per_thread)
        #queue.put(threadidx, histdata_per_thread)


    def AutoScrape(self, pairs='all', begin_datetime=None, local=True, shutdown_datetime=None, period=240, threadsnum=4):

        def __chunks__(l, n):
            """Yield successive n-sized chunks from l."""
            for i in range(0, len(l), n):
                yield l[i:i + n]

        if pairs == 'all':
            pairs = list(self.SupportPairs())
        else:
            pairs = [pairs] if type(pairs) != type([]) else pairs

        pairs_per_thread = list(__chunks__(pairs, len(pairs)//threadsnum+1))

        segbegin_timestamp = self.ConvertDateToTimeStamp(begin_datetime, local)

        shutdown_timestamp = None

        if shutdown_datetime != None:
            shutdown_timestamp  = self.ConvertDateToTimeStamp(shutdown_datetime, True)

        while True:

            now_timestamp = self.ConvertDateToTimeStamp(datetime.now(), True)
            if shutdown_timestamp != None and now_timestamp > shutdown_timestamp :
                break

            segend_timestamp = segbegin_timestamp + period * 60
            segend_timestamp = segend_timestamp if segend_timestamp <= now_timestamp else now_timestamp

            jobs = []
            queue = Queue()

            print(self.__AutoScrape_Driver__(queue, 0, pairs_per_thread[0], segbegin_timestamp, segend_timestamp))


#
            #for i in range(threadsnum):
            #    print(i)
            #    process = Process(target=self.__AutoScrape_Driver__, args=(queue, i, pairs_per_thread[i], segbegin_timestamp, segend_timestamp))
            #    jobs.append(process)
#
#
            #for j in jobs:
            #    j.start()
#
            #for j in josb:
            #    j.join()
#
            #exit()
            #print(queue.get())

    def ScrapeHistoryData(self, pairs='all', date_end=None, date_begin=None, local=True, write2db=False, rmrawdate=True):

        if pairs == 'all':
            pairs = list(self.SupportPairs())
        else:
            pairs = [pairs] if type(pairs) != type([]) else pairs

        for pair in pairs:

            date_end, date_begin = self.ConvertDateToTimeStamp([date_end, date_begin], local)
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
