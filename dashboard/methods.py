from library.parsing import parse_calls_from_db, get_from_clickhouse, split_dataframe_by_servers, \
    check_database_connection
from library.methods import system_is_linux, responses_info
from parameters.dashboard_parameters import replace_plots, config_file_path

from dash import dcc, dash_table
from dash_bootstrap_templates import load_figure_template
import dash_bootstrap_components as dbc

from plotly.express import scatter

import pandas as pd
import yaml

import logging
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
load_figure_template("darkly")


def get_clickhouse_connection_parameters(parameters_file=config_file_path) -> dict:
    """
    Get data for connection from file
    :param parameters_file: filepath
    :return: dictionary with parameters
    """
    try:
        with open(parameters_file) as f:
            parameters = yaml.safe_load(f)['clickhouse']
    except FileNotFoundError:
        logging.error('parameters file not found')
        parameters = None
    return parameters


def get_webapp_connection_parameters(parameters_file=config_file_path) -> dict:
    try:
        with open(parameters_file) as f:
            parameters = yaml.safe_load(f)['webapp']
    except FileNotFoundError:
        logging.error('Parameters file not found')
        parameters = None
    return parameters


def get_data(hours_of_calls_data=None, should_replace=True) -> list:
    """
    Method to get data from clickhouse and store data as list (element is dataframe by each server)
    :param hours_of_calls_data: time to get data (in hours)
    :param should_replace: additional parameter to change position of plots
    :return: (list of dataframes, response codes that are present)
    """
    connection_params = get_clickhouse_connection_parameters()

    if connection_params:
        connection = check_database_connection(db_usr=connection_params['clickhouse_user'],
                                               passwd=connection_params['clickhouse_password'],
                                               clickhouse_url=connection_params['clickhouse_url'],
                                               database=connection_params['clickhouse_database'])
    else:
        connection = False

    if not connection:
        servers_data = None
        logging.error('No connection with database')

    else:
        # SQL-QUERY TO GET CALLS DATA
        sql_query = (f"SELECT * FROM "
                     f"{connection_params['clickhouse_user']}.{connection_params['clickhouse_database']} "
                     f"WHERE datetime > NOW() - INTERVAL {int(hours_of_calls_data * 60)} MINUTE AND datetime < NOW()")
        if not system_is_linux():
            sql_query = (f"SELECT * FROM "
                         f"{connection_params['clickhouse_user']}.{connection_params['clickhouse_database']} "
                         f"ORDER by datetime DESC LIMIT {int(hours_of_calls_data * 3 * 60)}")
            # query for windows-systems is not accurate. datetime interval part of query should be corrected

        calls_data = get_from_clickhouse(db_usr=connection_params['clickhouse_user'],
                                         passwd=connection_params['clickhouse_password'],
                                         clickhouse_url=connection_params['clickhouse_url'],
                                         sql_query=sql_query)

        calls_dataframe = parse_calls_from_db(calls_data)

        servers_data = split_dataframe_by_servers(calls_dataframe)

        if should_replace:
            servers_data[1], servers_data[2] = servers_data[2], servers_data[1]

    return servers_data


def plots_initialization():
    """
    Get db info at initialization and create canvas
    :return:
    """
    graphs = []
    initial_servers_data = get_data(hours_of_calls_data=12)
    if initial_servers_data:
        graphs = [dcc.Graph(figure={}, id=f'plot_{n}',
                            config={'displaylogo': False,
                                    'modeBarButtonsToRemove': ['autoscale', 'lasso2d',
                                                               'select2d', 'zoomIn',
                                                               'zoomOut']
                                    }
                            )
                  for n in range(len(initial_servers_data))]
    if not graphs:
        logging.warning("Dashboard plots were not created")
    return graphs


def figure_constructor(data=None):
    """
    Make a figure for dashboard
    :param data: dataframe
    :return: one figure (statistics for server)
    """

    server = list(data['server'].unique())[0]
    codes = list(data.columns[:-2])
    codes.sort()

    code_labels = {}
    for code in codes:
        try:
            code_labels[code] = f'{code} <{responses_info()[int(code)][0]}>'
        except (ValueError, KeyError):
            code_labels[code] = code

    figure = scatter(data, x='time', y=codes,
                     labels={
                         'time': 'Datetime MSK (UTC +03)',
                         'value': 'Calls per minute',
                         'variable': 'Response code'
                     })
    figure.for_each_trace(lambda t: t.update(name=code_labels[t.name],
                                             legendgroup=code_labels[t.name],
                                             hovertemplate=t.hovertemplate.replace(t.name, code_labels[t.name])
                                             )
                          )
    figure.update_layout(
        title=dict(text=server, font=dict(size=20), automargin=True),
        font=dict(size=15)
    )

    return figure


def calls_statistics_tables(data=None):
    """
    Creates a table with stats by all servers
    :param data: list of servers dataframes
    :return: dash DataTable
    """
    output_layout = []
    for i, dat in enumerate(data):
        server_name = list(dat['server'].unique())[0]
        df = pd.DataFrame(dat.describe()).drop(['time'], axis=1)
        output_layout.append(dbc.Label(server_name))
        output_layout.append(
            dash_table.DataTable(df.to_dict('records'), [{"name": i, "id": i} for i in df.columns], id=f'table_{i}'))
    return output_layout


def response_code_button():
    """
    Button to show and hide 200 response codes
    :return:
    """
    button = dbc.Button('Loading...',
                        outline=True,
                        color='secondary',
                        id='response-code-button',
                        n_clicks=0,
                        style={'height': '40px', 'width': '200px', "margin-left": "15px"})
    return button


def auto_refresh_button():
    """
    Button to enable and disable page auto-refresh
    :return: dash button
    """
    button = dbc.Button('Auto Refresh Active',
                        outline=False,
                        color='secondary',
                        id='auto-refresh-button',
                        n_clicks=0,
                        style={'height': '40px', 'width': '200px', "margin-left": "15px"})
    return button


def time_interval_dropdown_menu():
    """
    Dropdown menu to choose time interval to show
    :return:
    """
    dropdown_menu = dcc.Dropdown(value='2d',
                                 options=[
                                     {'label': '1 day', 'value': '1d'},
                                     {'label': '2 days', 'value': '2d'},
                                     {'label': '1 week', 'value': '7d'},
                                     {'label': '2 weeks', 'value': '14d'},
                                     {'label': '1 month', 'value': '30d'}
                                 ],
                                 clearable=False,
                                 id='time-interval-dropdown-menu',
                                 style={'height': '40px', 'width': '200px', 'color': 'black'})
    return dropdown_menu


def page_auto_refresh(seconds=None):
    """
    Page auto-refresh functionality
    :return:
    """
    return [dcc.Interval(id='interval-component',
                         interval=seconds * 1000,
                         n_intervals=0,
                         disabled=False)]


def user_interface():
    """
    All UIX if dash-list
    :return:
    """
    interface_elements = [
        dbc.Row(
            [
                auto_refresh_button(),
                response_code_button(),
                time_interval_dropdown_menu()
            ], style={'margin-top': '15px', 'margin-left': '20px'}
        )
    ]
    return interface_elements
