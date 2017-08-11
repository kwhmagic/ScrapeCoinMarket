from time import mktime
from datetime import datetime, timedelta
from poloniex import Poloniex
from twisted.internet import task
from twisted.internet import reactor

polo = Poloniex()

# Relative to local time.
POLO_TIME_ZONE = 8;

# The format of date is "%Y-%m-%d %H:%M:%S"
def convert_strdate_to_timestamp(date):

    unix_timestamp = mktime(date.timetuple())
    
    return unix_timestamp

# period (minutes)
def scrpe_trade_history(date_begin, date_end, timezone, period):

    time_begin = datetime.strptime(date_begin, "%Y-%m-%d %H:%M:%S") + timedelta(hours=8)
    time_end   = datetime.strptime(date_end  , "%Y-%m-%d %H:%M:%S") + timedelta(hours=8)

    timestamp_end = convert_strdate_to_timestamp(time_end)

    pre_time, time = time_begin, time_begin
    pre_timestamp = convert_strdate_to_timestamp(time_begin)

    while time < time_end:

        tmptime = time + timedelta(minutes=period)
        
        if tmptime > time_end:
            tmptime = time_end

        tmptimestamp = convert_strdate_to_timestamp(tmptime)

        history_data = polo.marketTradeHist('BTC_ETH', pre_timestamp, tmptimestamp)

        if(len(history_data) > 50000):

            period /= 2;

        else:

            pre_time_str, tmptime_str = pre_time.strftime("%Y-%m-%d %H:%M:%S"), tmptime.strftime("%Y-%m-%d %H:%M:%S")

            print("From " + pre_time_str + " To " + tmptime_str + "Spread min: %d min." % period)
            print(len(history_data))

            if(tmptime == time_end):
                break

            time += timedelta(minutes=period)
            pre_time, pre_timestamp = tmptime, tmptimestamp

    exit()

scrpe_trade_history("2017-08-07 06:17:00", "2017-08-09 08:18:00", 8, 120)
