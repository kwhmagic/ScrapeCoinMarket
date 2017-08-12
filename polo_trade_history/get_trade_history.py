import os
from time import mktime
from datetime import datetime, timedelta
from poloniex import Poloniex
from pandas import DataFrame
from twisted.internet import task
from twisted.internet import reactor

polo = Poloniex()

DATAROOT = 'polo_history_data'
LOGROOT  = 'log'

# The format of date is "%Y-%m-%d %H:%M:%S"

def get_pair_names():

    pairs = polo.returnTicker().keys()

    return pairs

def convert_datestr_to_datetime(date_str, date_format):

    date = datetime.strptime(date_str, date_format)
    
    return date

# The format of date is "%Y-%m-%d %H:%M:%S"
def convert_datetime_to_timestamp(date):

    unix_timestamp = mktime(date.timetuple())
    
    return unix_timestamp


def relative_timespread():

    local_time = datetime.now()
    polo_time  = convert_datestr_to_datetime(polo.marketTradeHist('BTC_ETH')[0]['date'], "%Y-%m-%d %H:%M:%S")
    deltatime_btw_local_remote = local_time - polo_time

    relative_timespread = 0;

    relative_hours, remainder = divmod(deltatime_btw_local_remote.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if( minutes > 50):
        relative_hours += 1;

    return timedelta(hours=relative_hours)

def scrape_log(pair_name, timelabel,begin_polo_time_str, end_polo_time_str, history_data_size, period, pnum):

    if not os.path.isdir(LOGROOT):

        os.mkdir(LOGROOT)

    PAIRLOGPATH = LOGROOT + "/" + pair_name

    if not os.path.isdir(PAIRLOGPATH):

        os.mkdir(PAIRLOGPATH)

    logfname = PAIRLOGPATH + "/" + pair_name + timelabel + ".log"

    logfp = open(logfname, 'a')

    if(pnum == 0):
        logfp.write("From,To,TradesNumber,SpreadMinutes\n")

    logfp.write(begin_polo_time_str + "," + end_polo_time_str + (",%d" % history_data_size) + (",%d\n" % period))

    logfp.close()

def write_row_hisdata_to_csv(fname, history_data, pnum):

    colume_names = history_data[0].keys()

    list_2d = []

    for row in history_data:

        row_list = []

        for key, value in row.iteritems():

            row_list.append(value)

        list_2d.append(row_list)

    df = DataFrame(list_2d, columns=colume_names)

    if pnum == 0:
        print_header = True
    else:
        print_header = False

    with open(fname, 'a') as fp:

        df.to_csv(fp, index=False, header=print_header, encoding='utf-8')


# period (minutes)
def scrpe_trade_history(pair_name, date_end_str, date_begin_str, period):

    if not os.path.isdir(DATAROOT):

        os.mkdir(DATAROOT)

    PAIRDATAPATH = DATAROOT + '/' + pair_name

    if not os.path.isdir(PAIRDATAPATH):

        os.mkdir(PAIRDATAPATH)

    timelabel = date_end_str.replace("-","").replace(" ","").replace(":","") + "-" + date_begin_str.replace("-","").replace(" ","").replace(":","")
    ofname = PAIRDATAPATH + "/" + pair_name + timelabel + ".csv"

    timespread = relative_timespread()

    time_end   = convert_datestr_to_datetime(date_end_str  , "%Y-%m-%d %H:%M:%S") + timespread 
    time_begin = convert_datestr_to_datetime(date_begin_str, "%Y-%m-%d %H:%M:%S") + timespread 

    timestamp_end = convert_datetime_to_timestamp(time_end)

    pre_time, time = time_end, time_end
    pre_timestamp = convert_datetime_to_timestamp(time_end)

    pnum = 0

    while time > time_begin:

        tmptime = time - timedelta(minutes=period)
        
        if tmptime < time_begin:
            tmptime = time_begin

        tmptimestamp = convert_datetime_to_timestamp(tmptime)

        history_data = polo.marketTradeHist(pair_name, tmptimestamp, pre_timestamp)

        if(len(history_data) > 50000):

            period /= 2;

        else:

            begin_polo_time_str, end_polo_time_str = (pre_time - timespread).strftime("%Y-%m-%d %H:%M:%S"), (tmptime - timespread).strftime("%Y-%m-%d %H:%M:%S")

            if(tmptime == time_end):
                break
            
            if len(history_data) != 0:
                write_row_hisdata_to_csv(ofname, history_data, pnum)

            scrape_log(pair_name, timelabel, begin_polo_time_str, end_polo_time_str, len(history_data), period, pnum)

            time -= timedelta(minutes=period)
            pnum += 1
            pre_time, pre_timestamp = tmptime, tmptimestamp


pairs          = get_pair_names()
date_end_str   = "2017-08-09 08:18:00"
date_begin_str = "2017-08-09 04:17:00"
period         = 120  #2hr

for pair in pairs:
    scrpe_trade_history(pair, date_end_str, date_begin_str,  120)

