import numpy as np
import base64
from io import BytesIO
from library.methods import calls_statistics, responses_info
from bs4 import BeautifulSoup as bs

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

matplotlib.use('Agg')


def one_server_plot(dataframe, axis, unique_codes, server, remove_ok_code=None,
                    colormap='RdYlGn_r'):
    """
    Plot calls of one server
    :param dataframe: full_dataframe
    :param axis: axis where to make plot
    :param unique_codes: list of unique response codes
    :param server: server name to plot
    :param remove_ok_code: should 200 code be removed
    :param colormap: to color different codes
    """
    responses_dict = responses_info()
    responses_codes = [str(code) for code in list(responses_dict.keys())]

    cmap = plt.cm.get_cmap(colormap, len(responses_codes))

    unique_codes_ = unique_codes.copy()
    if remove_ok_code:
        unique_codes_.remove('200')

    for code in sorted(unique_codes_):
        if dataframe[code].max() < 1:
            continue

        try:
            label_code = f'{code} <{responses_dict[int(code)][0]}>'
            color = cmap(responses_codes.index(code))
        except ValueError:
            label_code = code
            color = 'grey'

        calls_max, _, calls_mean, _, calls_sum = calls_statistics(dataframe, code)

        legend_additional_info = f' ({calls_sum} times per hour)' if code != '200' else f' | MEAN: {calls_mean}'
        plot_label = f'Response: {label_code} \nMAX: {calls_max}' + legend_additional_info

        axis.plot(dataframe['time'], dataframe[code], alpha=0.15, color=color)
        axis.scatter(dataframe['time'], dataframe[code],
                     marker='.', s=15, edgecolor=color, color='white',
                     label=plot_label)

    if unique_codes and not remove_ok_code:
        axis.set_ylim(ymin=1)

    # MSK DATETIME AXIS LABELS
    datetime_format = mdates.DateFormatter('%d-%b %H:%M')
    axis.xaxis.set_major_formatter(datetime_format)
    axis.yaxis.set_major_locator(MaxNLocator(integer=True))
    axis.set_xlabel(r'Datetime MSK (UTC +03)')

    # COMMON PLOT SETTINGS
    axis.grid(visible=True, axis='both', which='both', alpha=0.2)
    axis.set_title(server, loc='right')
    axis.legend(edgecolor='black', shadow=False,
                loc='upper left',
                bbox_to_anchor=(1.01, 1.0))
    axis.set_ylabel(r'Calls per minute', fontstyle='italic')


def all_servers_plot(calls_dataframe, remove_ok_code=False):
    """
    Plot calls data for all servers
    :param calls_dataframe: full dataframe of calls data
    :param remove_ok_code: should 200 code be removed (for one_plot_function)
    :return: figure of all servers calls data
    """
    servers = list(calls_dataframe['server'].unique())
    codes = list(calls_dataframe.columns[:-2])

    n_servers = len(servers)
    n_cols = 1
    figure, axes = plt.subplots(ncols=n_cols, nrows=n_servers, figsize=(18, 8))

    try:
        axes = axes.flatten()
    except AttributeError:
        axes = np.array([axes])

    servers = sorted(servers)

    for i, server in enumerate(servers):
        server_df = calls_dataframe[calls_dataframe['server'] == server]
        one_server_plot(server_df, axes[i], codes, server, remove_ok_code=remove_ok_code)

    figure.tight_layout()

    return figure


def publish_plot(fig, html_page) -> str:
    """
    Add plot to html-parsed page
    :param fig: figure of plots
    :param html_page:
    :return: html-page in string format
    """
    buf = BytesIO()

    fig.savefig(buf, format="png")
    plt.clf()
    plt.close()

    plot_data = base64.b64encode(buf.getbuffer()).decode("ascii")

    with open(html_page, 'r') as page:
        html_doc = page.read()
    page.close()

    modified_data = bs(html_doc, 'html.parser')

    for tag in modified_data.findAll("img"):
        tag['src'] = f"data:image/png;base64,{plot_data}"  # f"<img src='data:image/png;base64,{plot_data}'/>"

    return str(modified_data)
