import dash
from dash import html, callback, Output, Input, State
import dash_bootstrap_components as dbc

from dashboard.methods import user_interface, plots_initialization, page_auto_refresh, get_data, \
    figure_constructor, get_webapp_connection_parameters
from library.methods import system_is_linux

import logging

logging.basicConfig(level=logging.INFO,
                    handlers=[
                        logging.FileHandler("parameters/webapp.log"),
                        logging.StreamHandler()
                    ],
                    format="%(asctime)s %(levelname)s: %(message)s")

interface = user_interface()
free_space = [
    dbc.Row([html.Br()]),
    dbc.Row([html.Br()], style={'backgroundColor': '#303030'})
]
plots = plots_initialization()
page_refresh = page_auto_refresh(seconds=60)
dash_interface = interface + free_space + plots + page_refresh

app = dash.Dash(__name__,
                requests_pathname_prefix='/calls_data/' if system_is_linux() else None,
                external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.themes.DARKLY])
app.layout = html.Div(dash_interface)
app.title = 'Calls Monitoring'
logging.getLogger('werkzeug').setLevel(logging.ERROR)

figures_output = [Output(component_id=f'plot_{n}', component_property='figure') for n in
                  range(len(plots_initialization()))]
response_button_output = [Output('response-code-button', 'outline'),
                          Output('response-code-button', 'children'),
                          Output('response-code-button', 'color')]
all_output = figures_output + response_button_output


@callback(
    all_output,
    Input(component_id='response-code-button', component_property='n_clicks'),
    Input(component_id='time-interval-dropdown-menu', component_property='value'),
    Input(component_id='interval-component', component_property='n_intervals'),
)
def plots_and_response_code_button(n_clicks, chosen_interval_value, n_intervals):
    time_interval = 24 * int(chosen_interval_value[:-1]) if chosen_interval_value is not None else 48
    servers_data = get_data(hours_of_calls_data=time_interval)
    if n_clicks % 2 == 0:
        button_params = [False, '200 <OK> Enabled', 'success']
    else:
        servers_data = [df.drop(['200'], axis=1) for df in servers_data]
        button_params = [True, '200 <OK> Disabled', 'secondary']

    figures = [figure_constructor(df) for df in servers_data] if servers_data else []

    return figures + button_params


@app.callback(
    [
        Output("interval-component", "disabled"),
        Output('auto-refresh-button', 'outline'),
        Output('auto-refresh-button', 'children'),
        Output('auto-refresh-button', 'color')
    ],
    [Input("auto-refresh-button", "n_clicks")],
    [State("interval-component", "disabled")],
)
def toggle_auto_refresh(n_clicks, disabled):
    if n_clicks % 2 == 0:
        return False, False, 'Auto Refresh is Active', 'primary'
    else:
        return True, True, 'Auto Refresh Disabled', 'secondary'


if __name__ == '__main__':

    connection_parameters = get_webapp_connection_parameters()
    host = connection_parameters['app_host']
    port = connection_parameters['app_port']

    logging.info(f"Trying to publish an application as {host}:{port}")

    try:
        int(connection_parameters['app_port'])
    except (ValueError, TypeError):
        logging.error(f"Port should be an integer from 1 to 65535")
        exit(0)

    app.run_server(debug=False,
                   host=host,
                   port=port)
