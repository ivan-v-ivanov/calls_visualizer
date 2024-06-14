import os

os.environ['OPENBLAS_NUM_THREADS'] = '1'

import logging
import subprocess
import pandas as pd
from datetime import datetime
from .methods import system_is_linux


def check_database_connection(db_usr=None,
                              passwd=None,
                              clickhouse_url=None,
                              database=None) -> bool:
    """
    Checks availability of clickhouse DB
    :param db_usr: database user
    :param passwd: database password
    :param clickhouse_url: url to database if following format: http://db-address:db-port
    :param database: database with data
    :return:
    """
    connection = True
    clickhouse_connect = os.path.join(f"{clickhouse_url}",
                                      f"database={db_usr}?user={db_usr}&password={passwd}")

    sql_query = f"SELECT table_name FROM information_schema.tables WHERE table_name LIKE '{database}'"
    echo_substring = f"echo {sql_query}" if not system_is_linux() else f"echo \"{sql_query}\""

    ps = subprocess.Popen(
        f"{echo_substring} | curl \"{clickhouse_connect}\" -d @-",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )

    ps_output = str(ps.communicate()[0])[2:-1]
    ps_output = list(filter(None, ps_output.split(r'\n')))

    if not ps_output:
        logging.error(f"table '{database}' does not exist")
        connection = False

    if len(ps_output) == 0 or ps_output[0] != database:
        logging.error(f"wrong connection/authentication with ${clickhouse_url}database=${database}?user=...&password...")
        connection = False

    return connection


def get_from_clickhouse(db_usr=None,
                        passwd=None,
                        clickhouse_url=None,
                        sql_query=None) -> list:
    """
    Get calls data from clickhouse
    :param db_usr: database user
    :param passwd: database password
    :param clickhouse_url: url to database if following format: http://db-address:db-port
    :param sql_query: SQL query (SELECT) to get necessary data
    :return: table from DB in list format (list_element is a call)
    """
    clickhouse_connect = os.path.join(f"{clickhouse_url}",
                                      f"database={db_usr}?user={db_usr}&password={passwd}")

    echo_substring = f"echo {sql_query}" if not system_is_linux() else f"echo \"{sql_query}\""

    ps = subprocess.Popen(
        f"{echo_substring} | curl \"{clickhouse_connect}\" -d @-",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )

    ps_output = str(ps.communicate()[0])[2:-1]
    calls_data = list(filter(None, ps_output.split(r'\n')))

    return calls_data


def parse_calls_from_db(calls_data, datetime_format='%Y-%m-%d %H:%M:%S') -> pd.DataFrame:
    """
    Parse list of calls data list received from clickhouse
    :param calls_data: list of calls data (got from get_from_clickhouse(...) method)
    :param datetime_format: format of datetime in database
    :return: pandas.DataFrame of calls data (columns: ['datetime', 'server', 'code_1', 'code_2' ... 'code_N'])
            sorted by call received time
    """
    calls_times, calls_servers, calls_codes = [], [], []
    for call in calls_data:
        call = call.split(r'\t')

        try:
            time, server, codes = call
        except ValueError:
            continue

        time = datetime.strptime(time, datetime_format)
        codes = codes.split(';')

        codes_dict = {}
        for code in codes:
            codes_dict.update({code.split()[0][:-1]: float(code.split()[1])})

        calls_times.append(time)
        calls_servers.append(server)
        calls_codes.append(codes_dict)

    unique_codes = find_unique_codes(calls_codes)
    calls_data = calls_responses_correction(calls_codes, unique_codes)
    calls_data.update({'server': calls_servers, 'time': calls_times})
    calls_df = pd.DataFrame(data=calls_data)
    return calls_df.sort_values(by=['time'])


def split_dataframe_by_servers(dataframe) -> list:
    """
    Reformat dataframe to list of dataframes according to present servers
    :param dataframe: df of calls
    :return: list of dataframes
    """
    servers = list(dataframe['server'].unique())
    servers.sort()
    output_data = []
    for server in servers:
        df = dataframe[dataframe['server'] == server]
        df = df.dropna(axis=1, how='all')
        output_data.append(df)
    return output_data


def find_unique_codes(all_calls_codes) -> list:
    """
    Find unique response codes in analyzed time interval
    :param all_calls_codes: list with dictionaries with calls numbers
    :return: list of unique response codes
    """
    codes_dicts = list(set(sum([list(val.keys()) for val in all_calls_codes], [])))
    return codes_dicts


def calls_responses_correction(calls_responses, unique_codes) -> dict:
    """
    Corrects list of dicts of calls to fill necessary response codes with None if call existed in some time points
    Method needs for correct visualization
    :param calls_responses: list with dictionaries with calls numbers
    :param unique_codes: unique codes for necessary time interval
    :return: dictionary of calls
    """
    for call in calls_responses:
        for code in unique_codes:
            if code not in list(call.keys()):
                call.update({code: None})
    calls_responses = {key: [i[key] for i in calls_responses] for key in list(calls_responses[0].keys())}
    return calls_responses
