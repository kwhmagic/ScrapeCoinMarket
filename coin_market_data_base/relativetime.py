import pytz
from time import mktime
from datetime import datetime, timedelta
from tzlocal import get_localzone

class RelativeTime( object ):

    _platform_timezone = pytz.utc
    _local_timezone    = get_localzone()

    def __init__(self, pytzone=None):

        if pytzone:
            _platform_timezone = pytzone

    def ConvertDateToTimeStamp(self, dates, local):

        isStr, only_one_date = False, False

        if (type(dates) != type([])):
            only_one_date = True
            if (type(dates) == type(" ")):
                isStr = True
        else:
            if (type(dates[0]) == type(" ")):
                isStr = True

        dates = [dates] if only_one_date else dates

        unix_timestamps = []

        if local:
            for date in dates:
                date_datetime = datetime.strptime( date, "%Y-%m-%d %H:%M:%S") if isStr else date
                unix_timestamps.append(mktime(date_datetime.timetuple()))
        else:
            for date in dates:
                date_datetime = datetime.strptime( date, "%Y-%m-%d %H:%M:%S") if isStr else date
                date_datetime = date_datetime.replace(tzinfo=self._platform_timezone).astimezone(self._local_timezone)
                unix_timestamps.append(mktime(date_datetime.timetuple()))

        if only_one_date:
            return unix_timestamps[0]
        else:
            return unix_timestamps
