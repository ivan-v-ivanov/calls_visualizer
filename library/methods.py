import pandas as pd
import subprocess
from datetime import timedelta


def responses_info() -> dict:
    """
    Dictionary with info about all response codes

    :return:
    response_codes_dictionary responses_dict = {'code': (short_description, full_description), ...}
    """
    responses_dict = {
        100: ('Continue', 'Request received, please continue'),
        101: ('Switching Protocols',
              'Switching to new protocol; obey Upgrade header'),

        200: ('OK', 'Request fulfilled, document follows'),
        201: ('Created', 'Document created, URL follows'),
        202: ('Accepted',
              'Request accepted, processing continues off-line'),
        203: ('Non-Authoritative Information', 'Request fulfilled from cache'),
        204: ('No Content', 'Request fulfilled, nothing follows'),
        205: ('Reset Content', 'Clear input form for further input.'),
        206: ('Partial Content', 'Partial content follows.'),

        300: ('Multiple Choices',
              'Object has several resources -- see URI list'),
        301: ('Moved Permanently', 'Object moved permanently -- see URI list'),
        302: ('Found', 'Object moved temporarily -- see URI list'),
        303: ('See Other', 'Object moved -- see Method and URL list'),
        304: ('Not Modified',
              'Document has not changed since given time'),
        305: ('Use Proxy',
              'You must use proxy specified in Location to access this '
              'resource.'),
        307: ('Temporary Redirect',
              'Object moved temporarily -- see URI list'),

        400: ('Bad Request',
              'Bad request syntax or unsupported method'),
        401: ('Unauthorized',
              'No permission -- see authorization schemes'),
        402: ('Payment Required',
              'No payment -- see charging schemes'),
        403: ('Forbidden',
              'Request forbidden -- authorization will not help'),
        404: ('Not Found', 'Nothing matches the given URI'),
        405: ('Method Not Allowed',
              'Specified method is invalid for this server.'),
        406: ('Not Acceptable', 'URI not available in preferred format.'),
        407: ('Proxy Authentication Required', 'You must authenticate with '
                                               'this proxy before proceeding.'),
        408: ('Request Timeout', 'Request timed out; try again later.'),
        409: ('Conflict', 'Request conflict.'),
        410: ('Gone',
              'URI no longer exists and has been permanently removed.'),
        411: ('Length Required', 'Client must specify Content-Length.'),
        412: ('Precondition Failed', 'Precondition in headers is false.'),
        413: ('Request Entity Too Large', 'Entity is too large.'),
        414: ('Request-URI Too Long', 'URI is too long.'),
        415: ('Unsupported Media Type', 'Entity body in unsupported format.'),
        416: ('Requested Range Not Satisfiable',
              'Cannot satisfy request range.'),
        417: ('Expectation Failed',
              'Expect condition could not be satisfied.'),
        481: ('Call Leg/Transaction Does Not Exist', ''),
        487: ('Request terminated', 'Request was terminated by user'),
        500: ('Internal Server Error', 'Server got itself in trouble'),
        501: ('Not Implemented',
              'Server does not support this operation'),
        502: ('Bad Gateway', 'Invalid responses from another server/proxy.'),
        503: ('Service Unavailable',
              'The server cannot process the request due to a high load'),
        504: ('Gateway Timeout',
              'The gateway server did not receive a timely response'),
        505: ('HTTP Version Not Supported', 'Cannot fulfill request.')
    }
    return responses_dict


def calls_statistics(dataframe, code) -> tuple:
    """
    Calls statistics summarizer

    :param dataframe: pandas dataframe of calls data (columns: ['datetime', 'server', 'code_1', 'code_2' ... 'code_N'])
    :param code: response code calls to analyze
    :return: max_value, median_value, average_value, number_of_events, all_calls_number
    """
    calls_max = int(dataframe[code].max())
    calls_median = int(dataframe[code].median())
    calls_mean = int(dataframe[code].mean())
    calls_events = dataframe[code].astype(bool).sum(axis=0)
    calls_timedelta = (dataframe[[code, 'time']]['time'].max() - dataframe[[code, 'time']][
        'time'].min()) / pd.Timedelta('1h')
    calls_sum = round(dataframe[code].sum() / calls_timedelta, 2)
    return calls_max, calls_median, calls_mean, calls_events, calls_sum


def time_converter(calls_times, delta_hours=4, negative=False):
    """
    Convert time to necessary timezone
    (doesn't use in current realization)
    :param calls_times: list of calls_datetime
    :param delta_hours: hours to add
    :param negative: if delta_hours should subtract
    :return: shifted datetime
    """
    delta_value = - timedelta(hours=delta_hours) if negative else timedelta(hours=delta_hours)
    return [time + delta_value for time in calls_times]


def system_is_linux() -> bool:
    """
    Checks if current system is Linux
    :return: True if Linux; False if else
    """
    ps = subprocess.Popen(f"uname", shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    system_string = str(ps.communicate()[0])
    return True if 'linux' in system_string.lower() else False
