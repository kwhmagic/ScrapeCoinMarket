import os
import sys
sys.stdout.flush()
import pprint

from datetime import datetime, date, time
from multiprocessing import Process, Queue, Pool
from functools import partial

from poloniex import Poloniex
from relativetime import RelativeTime
from mktdbinfo import MktDBInfo

def Chunks(l, n):
    #Yield successive n-sized chunks from l.
    for i in range(0, len(l), n):
        yield l[i:i + n]

class PoloMktDB( MktDBInfo, RelativeTime ):

    def __init__(self, sqlcore, rootpath, pairs='all'):

        self._cleandata, self._rawdata = dict(), dict()
        self._sqlcore, self._rootpath, = sqlcore, rootpath
        self._pairs = list(Poloniex().returnTicker().keys()) if pairs=='all' else pairs

        for pair in self._pairs:
            self._cleandata[pair], self._rawdata[pair] = [], []

        MktDBInfo.__init__(self, "Poloniex", Poloniex(), self._sqlcore, self._rootpath, self._pairs)
        RelativeTime.__init__(self)

    def __reduce__(self):
        return (self.__class__, (self._sqlcore, self._rootpath, self._pairs))

    def SupportPairs(self, Saving=True):
        return  list(self._supportpairs)

    def RawData(self):
        return self.rawdata

    def CleanData(self):
        if len(self.cleandata) == 0 and len(self.rawdata) != 0:
            self.CleanRawData(delrawdata=False)
        return self.cleandata

    def __ScrapeDriver__( self, begin_timestamp, end_timestamp, pairs_last_idx, clean, pairs):

        histdata_per_thread, clean_per_thread, last_idx = dict(), dict(), dict()

        for pair in pairs:

            segbegin_timestamp, segend_timestamp = begin_timestamp, end_timestamp
            last_idx[pair] = pairs_last_idx[pair]
            histdata_per_thread[pair] = []
            clean_per_thread[pair] = []

            while True:

                segrange = segend_timestamp - segbegin_timestamp
                histdata_seg = self._platformobj.marketTradeHist(pair, segbegin_timestamp, segend_timestamp)

                if clean:

                    cleanrow = dict()
                    for row in reversed(histdata_seg):
                        b_or_s = row['type'][0]
                        cleanrow['buy_sell'] = 1 if b_or_s == 'b' else -1
                        for key, value in row.items():
                            if key[0] == 'd':
                                datestr, timestr = value.split(' ')
                                cleanrow['date'] = datetime.strptime( datestr, "%Y-%m-%d").date()
                                cleanrow['time'] = datetime.strptime( timestr, "%H:%M:%S").time()
                            elif key[0] in 'ar':
                                cleanrow[key] = float(value)
                            elif key == 'tradeID':
                                cleanrow[key] = value
                        clean_per_thread[pair].append(cleanrow.copy())

                else:
                    histdata_per_thread[pair] += histdata_seg[::-1]

                if len(histdata_seg) >= 50000:

                    latest_timestamp = self.ConvertDateToTimeStamp(histdata_seg[0]['date'], False)
                    segend_timestamp = latest_timestamp

                if int(segend_timestamp) >= int(end_timestamp):
                    break

                segbegin_timestamp, segend_timestamp = segend_timestamp, end_timestamp

        if clean:
            return (clean_per_thread, last_idx)
        else:
            return (histdata_per_thread, last_idx)


    def AutoScrape(self, pairs='all', begin_datetime=None, local=False, shutdown_datetime=None, period=240, process_num=4):

        if pairs == 'all':
            pairs = self.SupportPairs()
        else:
            pairs = [pairs] if type(pairs) != type([]) else pairs

        histdata_dict, pairs_last_idx = dict(), dict()

        for pair in pairs:
            histdata_dict[pair]  = []
            pairs_last_idx[pair] = 0

        chunksize = len(pairs)//process_num+1 if len(pairs) % process_num != 0 else len(pairs)//process_num
        pairs_per_thread = list(Chunks(pairs, chunksize))

        segbegin_timestamp = self.ConvertDateToTimeStamp(begin_datetime, local)

        shutdown_timestamp = None

        if shutdown_datetime != None:
            shutdown_timestamp  = self.ConvertDateToTimeStamp(shutdown_datetime, True)

        while True:

            now_timestamp = self.ConvertDateToTimeStamp(datetime.now(), True)

            segend_timestamp = segbegin_timestamp + period * 60
            segend_timestamp = segend_timestamp if segend_timestamp <= now_timestamp else now_timestamp

            pool = Pool(process_num)
            scrape_func = partial(self.__ScrapeDriver__, segbegin_timestamp, segend_timestamp, pairs_last_idx, True)
            itptr = pool.imap(scrape_func, pairs_per_thread)

            pool.close()
            pool.join()

            for it in itptr:
                for pair_name, pair_data in it[0].items():
                    self.Add_All(pair_name, pair_data, reverse=False)
                    self.New(pair_name)
                    self.Commit(pair_name)

            if shutdown_timestamp != None and now_timestamp > shutdown_timestamp :
                break


    def ScrapeHistoryData(self, pairs='all', date_begin=None, date_end=None, period=480, local=True, write2db=False, rmrawdate=True, process_num=4):

        if pairs == 'all':
            pairs = self._supportpairs
        else:
            pairs = [pairs] if type(pairs) != type([]) else pairs

        pairs_last_idx = dict()

        for pair in pairs:
            pairs_last_idx[pair] = 0

        chunksize = len(pairs)//process_num+1 if len(pairs) % process_num != 0 else len(pairs)//process_num
        pairs_per_thread = list(Chunks(pairs, chunksize))

        begin_timestamp , end_timestamp = self.ConvertDateToTimeStamp([date_begin, date_end], local)
        segbegin_timestamp = begin_timestamp
        segend_timestamp = segbegin_timestamp + period * 60
        segend_timestamp = segend_timestamp if segend_timestamp < end_timestamp else end_timestamp

        cnt = 0

        while True:

            if segbegin_timestamp >= end_timestamp:
                break

            pool = Pool(process_num)
            scrape_func = partial(self.__ScrapeDriver__, segbegin_timestamp, segend_timestamp, pairs_last_idx, True)

            itptr = pool.imap(scrape_func, pairs_per_thread)

            segbegin_timestamp, segend_timestamp = segend_timestamp, segbegin_timestamp + period * 60
            segend_timestamp = segend_timestamp if segend_timestamp < end_timestamp else end_timestamp

            if write2db:
                for it in itptr:
                    for pair_name, pair_data in it[0].items():
                        self.Add_All(pair_name, pair_data, reverse=False)
                        self.New(pair_name)
                        self.Commit(pair_name)

    def Add(self, pair, item):
        self.__Add__( pair, self._pairsDB[pair]._classmap(**item) )

    def Add_All(self, pair, itmes, reverse=False):

        mktobjs = []
        db = self._pairsDB.get(pair)
        if not reverse:
            for item in itmes:
                mktobjs.append(db._classmap(**item))
        else:
            for item in reversed(itmes):
                mktobjs.append(db._classmap(**item))

        self.__Add__All__(pair, mktobjs)

    def Delete(self, pair, item):
        self.__Delete__(pair, self._pairsDB[pair]._classmap(**item))

    def New(self, pair):
        self.__New__(pair)

    def Commit(self, pair):
        self.__Commit__(pair)

    def All(self, pair):
        return list(self.__All__(pair))

    def Count(self, pair):
        return self.__Count__(pair)

    """
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
    """
