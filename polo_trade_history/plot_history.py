import os
from time import mktime
#import datetime as dt
from datetime import datetime, timedelta
from poloniex import Poloniex
from pandas import DataFrame
import sys

import numpy as np
import pandas as pd
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import LinearLocator, FormatStrFormatter
import matplotlib.ticker as mticker
from matplotlib.finance import candlestick_ohlc
import matplotlib.dates as mdates
#from twisted.internet import task
#from twisted.internet import reactor

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
    print "CSV file: " + ofname
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
                #print "CSV file at: " + ofname
            scrape_log(pair_name, timelabel, begin_polo_time_str, end_polo_time_str, len(history_data), period, pnum)

            time -= timedelta(minutes=period)
            pnum += 1
            pre_time, pre_timestamp = tmptime, tmptimestamp

    return ofname


def price_ohlc(df, interval):
    
    #print "df = " ,df.head()
    #print "df.index = ", df.index
    #print "df['date'] = ", df['date'].head()
    #print "type of index = ", type(df.index)
    #print "type of df['date']", type(df['date'])
    #print "type of df['date']value", type(df['date'][1])
    
    #df['date'].to_pydatetime()
    #print type(df['date'])
    #df.index.to_pydatetime()
    #print type(df.index)
    #print df.index
    #df.index = pd.to_datetime(df.index)
    #print type(df.index)
    #df['date'] = pd.to_datetime(df['date'].values)
    #print "type of df['date']", tpye(df['date'].values)
    #df.set_index(df['date'])

    #df.set_index(pd.DatetimeIndex(pd.to_datetime(df['date'])))
    df_ohlc = df['rate'].resample(str(interval)+'Min').ohlc()
    #ohlc = df.resample(str(interval)+'Min', how={'rate': 'ohlc'})
    #ohlc = df.resample(str(interval)+'Min', how={'rate': 'ohlc'})
    return df_ohlc


def type_trade_modify(df):
    # Clean the data of 'buy' and 'sell'
    df['type_trade'] = 0
    condition = df['type'] == 'buy'
    df.loc[condition, 'type_trade'] = 1
    df.loc[~condition, 'type_trade'] = -1
    df['buy_trade'] = df['type_trade'] * df['total']
    df['sell_trade'] = df['type_trade'] * df['total']

    condition_plus = df['buy_trade'] > 0
    df.loc[~condition_plus, 'buy_trade'] = 0
    condition_minus = df['sell_trade'] < 0
    df.loc[~condition_minus, 'sell_trade'] = 0
    
    return df


def plot_candle(ofname, interval):

    #df = pd.read_csv(ofname, index_col='date', parse_dates=True)
    #print type(df['date'])
    #print type(df['date'][0])
    #print type(df.index)
    #print type(df.index[0])
     
    parser = lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    #df = pd.read_csv(ofname, index_col=[3], sep=',',parse_dates=True, date_parser=parser, encoding='utf-8')
    df = pd.read_csv(ofname, index_col='date', sep=',', parse_dates=True, date_parser=parser)
    #print df.head()
    #df.drop(df.loc[0])
    #df.columns = ['tradeID','amount','rate','total','type','globalTradeID']
    #print df.head()
    #print df.dtypes
    #print df.info()
    #print df.head()
    #print df['total'].head()
    #df[['rate','amount','total']] = df[['rate','amount', 'total']].astype(float)
    #df['rate'].astype(float)
    #pd.to_numeric(df['rate'], errors='coerce')
    #print df.apply(pd.to_numeric).info
    #print df['rate'].dtype
    #print type(df['rate'])
    #df['amount'].astype(float)
    #print df['amount'].dtype
    #df['total'].astype(float)
    #print df['total'].dtype
    
    
    #print df.dtypes
    #print df['date'].head()
    #print df['date'].head()
    #print 'the element type = ', type(df.index)
    #print 'the rate type = ', df['rate'].dtype
    #pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S")
    #df['date'].to_datetime()
    #print type(df['date'])
    #print type(df['date'][0])
    #print df.head()
    #print df.index[0]
    #print type(df.index)
    #print type(df.index[0])
    #df.index.to_datetime()
    #print type(df.index)
    #print type(df.index[0])
    
    #print type(df['date'])
    #print type(df['date'][0])
    df = type_trade_modify(df)
    df_ohlc = price_ohlc(df, interval)
    

    #print df.head()
    # Resample the price into OHLC fromat
    #df.set_index('date')
    #df.reset_index().set_index('date')
    #df2 = df.set_index(pd.DatetimeIndex(pd.to_datetime(df.index)))
    #df_ohlc = df2['rate'].resample(str(interval)+'Min').ohlc()
    df_ohlc['buy_volume'] = df['buy_trade'].resample(str(interval)+'Min').sum()
    df_ohlc['sell_volume'] = df['sell_trade'].resample(str(interval)+'Min').sum()

    # Automatically adjust the histogram width
    time_max = df_ohlc.index.max()
    time_min = df_ohlc.index.min()
    num_sticks = len(df_ohlc.index)
    time_delta = time_max - time_min
    time_delta_in_day = time_delta.total_seconds() / 86400
    stick_width = 0.6 * time_delta_in_day / num_sticks

    # Reset the Dataframe index and calculate indicators
    df_ohlc = df_ohlc.reset_index()
    df_ohlc.columns = ["Date", "Open", "High", "Low", "Close", "Buy", "Sell"]

    df_ohlc['SMA'] = df_ohlc['Close'].rolling(window=10, center=False).mean()
    #df_ohlc['EMA'] = df_ohlc['Close'].ewm(span=10).mean()
    df_ohlc['STD'] = df_ohlc['Close'].rolling(window=10,center=False).std()
    df_ohlc['BBup'] = df_ohlc['SMA'] + 2*df_ohlc['STD']
    df_ohlc['BBdown'] = df_ohlc['SMA'] - 2*df_ohlc['STD']
    df_ohlc['EMA26'] = df_ohlc['Close'].ewm(span=26).mean()
    df_ohlc['EMA12'] = df_ohlc['Close'].ewm(span=12).mean()
    df_ohlc['MACD'] = df_ohlc['EMA12'] - df_ohlc['EMA26']
    df_ohlc['MACDema9'] = df_ohlc['MACD'].ewm(span=9).mean() 
    
    #print df_ohlc[['MACD', 'MACDema9']]

    #df_ohlc.to_csv("./test.csv")
    #print df_ohlc.head()

    # Reset Date format
    df_ohlc['Date'] = df_ohlc['Date'].map(mdates.date2num)


    # Plotting
    fig = plt.figure(figsize = (15,10) )
    ax1 = plt.subplot2grid((6,4), (0,0), rowspan=4, colspan=4)
    ax1.xaxis_date()
    ax2 = ax1.twinx()
    ax3 = plt.subplot2grid((6,4), (4,0), sharex=ax1, rowspan=2, colspan=4)

    ax1.set_xlabel("Time", fontsize=14)
    ax1.set_ylabel("Price", fontsize=14)
    ax2.set_ylabel("Volume", fontsize=14)
    ax3.set_ylabel("MACD", fontsize=14)
    
    # Plot Volume Histogram
    ax2.bar(df_ohlc['Date'], df_ohlc['Buy'], width=stick_width, color='green', alpha=0.2,linewidth=0, align='center')
    ax2.bar(df_ohlc['Date'], df_ohlc['Sell'], width=stick_width, color='red', alpha=0.2,linewidth=0, align='center')
    
    # Plot Bollinger Band 
    #ax1.plot(df_ohlc['Date'], df_ohlc['BBup'], 'gray', alpha=0.5)
    #ax1.plot(df_ohlc['Date'], df_ohlc['BBdown'], 'gray', alpha=0.5)
    ax1.plot(df_ohlc['Date'], df_ohlc['SMA'], 'k-',alpha=0.5, lw=2)
    ax1.fill_between(df_ohlc['Date'], df_ohlc['BBup'], df_ohlc['BBdown'], facecolor='cyan', alpha=0.1, linewidth=0,interpolate=True)
    
    # Plot candlestick
    candlestick_ohlc(ax1,df_ohlc.values,width=stick_width, colorup='g', colordown='r',alpha=0.5)

    # Plot MACD
    ax3.plot(df_ohlc['Date'], df_ohlc['MACD'], color='green', lw=2)
    ax3.plot(df_ohlc['Date'], df_ohlc['MACDema9'], color='red', lw=1)
    
    #ax3.relim()
    #ax3.autoscale_view()
    #ax3.set_ylim(-30,30)
    ax3.fill_between(df_ohlc['Date'], df_ohlc['MACD']-df_ohlc['MACDema9'], 0, facecolor='blue', alpha=0.1, linewidth=1)
    
    # Plot out as png file
    pngname = os.path.splitext(ofname)[0]
    pngname = pngname.rpartition('/')[-1]
    
    #pngname = ofname.rpartition('/')[-1]
    #pngname.rpartition('.')[0]
    pngname = pngname + '.png'
    plt.savefig(pngname,dpi=300)
    #plt.show()




#pairs          = get_pair_names()
#pairs          = ['USDT_LTC']
#date_end_str   = "2017-08-31 18:55:00"
#date_begin_str = "2017-08-31 14:30:00"
period         = 240  #2hr
avg_interval = 5  # interval of moving average  


pair_args = [str(sys.argv[1]) + "_" + str(sys.argv[2])]
pairs = pair_args
date_begin_args = "20" + str(sys.argv[3][0:2]) + "-" + str(sys.argv[3][2:4]) + "-" + str(sys.argv[3][4:6]) + " " + str(sys.argv[3][6:8]) + ":" + str(sys.argv[3][8:10]) + ":00"

date_end_args = "20" + str(sys.argv[4][0:2]) + "-" + str(sys.argv[4][2:4]) + "-" + str(sys.argv[4][4:6]) + " " + str(sys.argv[4][6:8]) + ":" + str(sys.argv[4][8:10]) + ":00"

print pair_args
print "Start time:  " + date_begin_args
print "Finish time: " + date_end_args

for pair in pairs:
    scrpe_file = scrpe_trade_history(pair, date_end_args, date_begin_args, period)
    plot_candle(scrpe_file, avg_interval)
    # old usage
    #scrpe_trade_history(pair, date_end_str, date_begin_str,  period)

