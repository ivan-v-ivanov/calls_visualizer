import os
from flask import Flask
from flaskapp.visualization import all_servers_plot, publish_plot
from library.parsing import get_from_clickhouse, parse_calls_from_db
from library.methods import system_is_linux
from parameters import *

host = '0.0.0.0'
port = 5004

workdir = os.path.dirname(os.path.realpath(__file__))
html_page = os.path.join(workdir, 'assets', 'index.html')

# SQL-QUERY TO GET CALLS DATA
hours_of_calls_data = 48  # hours
sql_query = f"YOUR SQL QUERY"
if not system_is_linux():
    sql_query = f"YOUR SQL QUERY FOR WINDOWS"
    # query for windows-systems is not accurate. datetime interval part of query should be corrected

app = Flask(__name__)


# Aggregator of all methods to create a static picture of calls statistics
# Uses library methods
def create_calls_statistics_plot(ok_code_should_be_removed):
    calls_data = get_from_clickhouse(db_usr=db_usr,
                                     passwd=passwd,
                                     clickhouse_url=clickhouse_url,
                                     sql_query=sql_query)

    calls_dataframe = parse_calls_from_db(calls_data)
    statistics_figure = all_servers_plot(calls_dataframe, remove_ok_code=ok_code_should_be_removed)
    return statistics_figure


@app.route('/', methods=['GET'])
def plot_all():
    figure = create_calls_statistics_plot(ok_code_should_be_removed=False)
    return publish_plot(figure, html_page)


@app.route('/nook', methods=['GET'])
def plot_without_ok():
    figure = create_calls_statistics_plot(ok_code_should_be_removed=True)
    return publish_plot(figure, html_page)


if __name__ == '__main__':
    app.run(host=host, port=port)
