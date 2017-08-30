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

        self._bufferdata = dict()
        self._sqlcore, self._rootpath, = sqlcore, rootpath
        self._pairs = list(Poloniex().returnTicker().keys()) if pairs=='all' else pairs
        self._pairs = [self._pairs] if type(self._pairs) != type([]) else self._pairs

        for pair in self._pairs:
            self._bufferdata[pair] = []

        MktDBInfo.__init__(self, "Poloniex", Poloniex(), self._sqlcore, self._rootpath, self._pairs)
        RelativeTime.__init__(self)

        self._last_timestamp, self._last_tradeid = dict(), dict()
        self.__InitLastTimeStampAndTradeId__(self._last_timestamp, self._last_tradeid, self._pairs)

    def __reduce__(self):
        return (self.__class__, (self._sqlcore, self._rootpath, self._pairs))

    def __InitLastTimeStampAndTradeId__(self, last_timestamp, last_tradeid, pairs):

        for pair in pairs:
            if self.__First__(pair) != None:
                last_row = self.__Last__(pair)
                last_tradeid[pair] = last_row.tradeID
                date, time = last_row.date, last_row.time
                last_timestamp[pair] = \
                self.ConvertDateToTimeStamp(datetime(date.year, date.month, date.day, time.hour, time.minute, time.second), False)
            else:
                last_tradeid[pair] = -1
                last_timestamp[pair] = -1

    # Return the difference.
    # diff < 1: orverlap
    # diff == 1: good
    # diff > 1: sperate
    def __CheckLastTradeId__(self, last_tradeid, first_input_tradeId):
        diff = first_input_tradeId - last_tradeid if last_tradeid != -1 else 1
        return diff

    def __CheckLastTimeStamp__(self, last_timestamp, first_input_timestamp):
        begin_timestamp = 0
        if last_timestamp != -1:
            begein_timestamp = first_input_timestamp if last_timestamp < first_input_timestamp else last_timestamp
        else:
            begin_timestamp = first_input_timestamp
        return begein_timestamp

    # pairs_last_idx: dict()[pair_name]
    def __ScrapeDriver__( self, begin_timestamp, end_timestamp, pairs_last_tradeid, clean, pairs):

        histdata_per_thread, clean_per_thread = dict(), dict()

        for pair in pairs:

            segbegin_timestamp, segend_timestamp = begin_timestamp, end_timestamp
            histdata_per_thread[pair] = []
            clean_per_thread[pair] = []

            last_tradeid = pairs_last_tradeid[pair]

            while True:

                segrange = segend_timestamp - segbegin_timestamp
                histdata_seg = self._platformobj.marketTradeHist(pair, segbegin_timestamp, segend_timestamp)

                if len(histdata_seg) != 0:

                    first_input_tradeId = histdata_seg[-1]['tradeID']
                    diff = self.__CheckLastTradeId__(last_tradeid, first_input_tradeId)
                    inc_row   = diff-1 if diff < 1 else 0
                    inc_clean = diff-1 if diff < 1 else len(histdata_seg)
                    last_tradeid = histdata_seg[0]['tradeID']

                    if clean:
                        cleanrow = dict()
                        for row in reversed(histdata_seg[:inc_clean]):
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
                        histdata_per_thread[pair] += histdata_seg[len(histdata_seg)-1+inc_row::-1]

                    if len(histdata_seg) >= 50000:
                        latest_timestamp = self.ConvertDateToTimeStamp(histdata_seg[0]['date'], False)
                        segend_timestamp = latest_timestamp

                if int(segend_timestamp) >= int(end_timestamp):
                    break

                segbegin_timestamp, segend_timestamp = segend_timestamp, end_timestamp

        if clean:
            return clean_per_thread
        else:
            return histdata_per_thread


    def AutoScrape(self, pairs='all', begin_datetime=None, local=False, shutdown_datetime=None, period=240, process_num=4):

        if pairs == 'all':
            pairs = self.SupportPairs()
        else:
            pairs = [pairs] if type(pairs) != type([]) else pairs

        histdata_dict = dict()

        for pair in pairs:
            histdata_dict[pair]  = []


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

            pairs_last_tradeid = self._last_tradeid.copy()

            pool = Pool(process_num)

            scrape_func = \
            partial(self.__ScrapeDriver__, segbegin_timestamp, segend_timestamp, pairs_last_tradeid, True)

            itptr = pool.imap(scrape_func, pairs_per_thread)

            pool.close()
            pool.join()

            segbegin_timestamp = segend_timestamp

            for it in itptr:
                for pair_name, pair_data in it.items():
                    if len(pair_data) != 0:
                        last_date, last_time = pair_data[-1]['date'], pair_data[-1]['time']
                        self._last_timestamp[pair_name] = \
                        self.ConvertDateToTimeStamp(datetime(last_date.year, last_date.month, last_date.day, last_time.hour, last_time.minute, last_time.second), False)
                        self._last_tradeid[pair_name] = pair_data[-1]['tradeID']
                        self.Add_All(pair_name, pair_data, reverse=False)
                        self.New(pair_name)
                        self.Commit(pair_name)

            if shutdown_timestamp != None and now_timestamp > shutdown_timestamp :
                break


    def ScrapeHistoryData(self, pairs='all', date_begin=None, date_end=None, period=480, local=True, write2db=False, clean=True, process_num=4):

        if pairs == 'all':
            pairs = self._supportpairs
        else:
            pairs = [pairs] if type(pairs) != type([]) else pairs

        chunksize = len(pairs)//process_num+1 if len(pairs) % process_num != 0 else len(pairs)//process_num
        pairs_per_thread = list(Chunks(pairs, chunksize))

        begin_timestamp , end_timestamp = self.ConvertDateToTimeStamp([date_begin, date_end], local)
        segbegin_timestamp = begin_timestamp
        segend_timestamp = segbegin_timestamp + period * 60
        segend_timestamp = segend_timestamp if segend_timestamp < end_timestamp else end_timestamp

        while True:

            if segbegin_timestamp >= end_timestamp:
                break

            pairs_last_timestamp, pairs_last_tradeid = self._last_timestamp, self._last_tradeid

            pool = Pool(process_num)
            scrape_func = partial(self.__ScrapeDriver__, segbegin_timestamp, segend_timestamp, pairs_last_tradeid, clean)

            itptr = pool.imap(scrape_func, pairs_per_thread)

            segbegin_timestamp, segend_timestamp = segend_timestamp, segend_timestamp + period * 60
            segend_timestamp = segend_timestamp if segend_timestamp < end_timestamp else end_timestamp

            if write2db:
                if clean:
                    for it in itptr:
                        for pair_name, pair_data in it.items():
                            if len(pair_data) != 0:
                                last_date, last_time = pair_data[-1]['date'], pair_data[-1]['time']
                                self._last_timestamp[pair_name] = \
                                self.ConvertDateToTimeStamp(datetime(last_date.year, last_date.month, last_date.day, last_time.hour, last_time.minute, last_time.second), False)
                                self._last_tradeid[pair_name] = pair_data[-1]['tradeID']
                                self.Add_All(pair_name, pair_data, reverse=False)
                                self.New(pair_name)
                                self.Commit(pair_name)
                else:
                    print("Can't write the dirty data into data base!!! [hint: clean=True]")
                    exit()
            else:
                for it in itptr:
                    for pair_name, pair_data in it.items():
                        if len(pair_data) != 0:
                            last_date, last_time = pair_data[-1]['date'], pair_data[-1]['time']
                            self._last_timestamp[pair_name] = \
                            self.ConvertDateToTimeStamp(datetime(last_date.year, last_date.month, last_date.day, last_time.hour, last_time.minute, last_time.second), False)
                            self._last_tradeid[pair_name] = pair_data[-1]['tradeID']
                            self.Add_All(pair_name, pair_data, reverse=False)
                            self._bufferdata[pair_name] += pair_data

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

    def First(self, pair):
        return self.__First__(pair)

    def Last(self, pair):
        return self.__Last__(pair)

    def QueryDate(self, pair, date_begin=None, date_end=None):

        data = []

        if date_begin != None and date_end == None:
            data += self._pairsDB[pair]._query.filter_by(date = date_begin)

        elif date_begin != None and date_end != None:
            data += self._pairsDB[pair]._query.filter(self._pairsDB[pair]._classmap.date>= date_begin, self._pairsDB[pair]._classmap.date < date_end)

        else:
            print("Err!!")
            exit()

        return data

    def SupportPairs(self, Saving=True):
        return  list(self._supportpairs)

    def ReturnBufferData(self):
        return self._bufferdata
