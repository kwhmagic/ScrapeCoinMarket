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
