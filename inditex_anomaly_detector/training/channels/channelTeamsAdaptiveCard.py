from inditex_anomaly_detector.training.channels.channel import Channel
from zcomcule_ml_commons.notifications import TeamsNotifications
from inditex_anomaly_detector.utils.logging_debugging import common_print
import requests
import json
import plotly.io as pio
from inditex_anomaly_detector.utils.config_utils import upload_plot_google
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from inditex_anomaly_detector.utils.format_utils import format_number
import plotly.graph_objects as go



class TeamsAdaptiveCardChannel(Channel):

    def __init__(self, name, config, plot_configuration, plot_credentials):
        super().__init__(name, TeamsNotifications(link=config['URL']), plot_configuration, plot_credentials)
        self._url = config['URL']
        self._parametrized_json = config['parametrized_card']

    def send(self, js):
        js = json.loads(js)
        common_print("json", js)
        headers = {'Content-Type': 'application/json'}
        h=requests.post(self._url, json=js, headers=headers)


class TeamsAdaptiveCardStockLowChannel(TeamsAdaptiveCardChannel):

    def __init__(self, name, config, plot_configuration, plot_credentials):
        super().__init__(name, config, plot_configuration, plot_credentials)

    def send(self, params):
        self.plot({
            'table': params['plot_table'],
            'dataframe_grouped_indexed': params['plot_dataframe_grouped_indexed']
        })

        js = self._parametrized_json.format( STOCK_SOURCE=           params['STOCK_SOURCE'],
                                             GRAPHICS=               params['GRAPHICS'] + "_stocklow.png",
                                             COLUMN_SETS=            params['COLUMN_SETS'])

        super().send(js)


    def plot(self, params):
        TMP_FILES_PATH="/tmp/"
        pio.templates.default = "plotly_white"
        plt.style.use('fivethirtyeight')
        plt.rcParams['lines.linewidth'] = 1.5
        pd.options.display.max_rows = 100000
        plots_file = str(round(params['table']['timestamp'][0].timestamp())) + str(params['table']['country_iso'][0]) + "_stocklow.png"
        plot_file_local_system = TMP_FILES_PATH+str(round(params['table']['timestamp'][0].timestamp())) + str(params['table']['country_iso'][0]) + "_stocklow.png"

        fig = plt.figure(figsize=(12, 6))
        if params['table']['z_score_indicador'][0] == 2:
            col = 'green'
        elif params['table']['z_score_indicador'][0] == 1:
            col = 'red'
        elif params['table']['z_score_indicador'][0] == 4:
            col = 'red'
        else:
            col = 'yellow'

        x = params['dataframe_grouped_indexed'][params['dataframe_grouped_indexed']['name_store'] == params['table']['name_store'][0]]['timestamp']
        y = params['dataframe_grouped_indexed'][params['dataframe_grouped_indexed']['name_store'] == params['table']['name_store'][0]]['qty']

        x1 = x.iloc[-1]
        y1 = y.iloc[-1]

        ax = plt.axes()
        plt.plot(x, y, linewidth=4, color='k')  # plot example
        plt.plot([x1], [y1], marker='o', markersize=15, color=col)
        plt.grid(axis='y')
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.yaxis.set_major_formatter(format_number)
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
        plt.xticks(fontsize=20)
        plt.yticks(fontsize=20)
        plt.show()  # for control
        fig.savefig(plot_file_local_system, dpi=fig.dpi)

        # upload to azure storage
        upload_plot_google( plot_file_local_system,
                            self.plot_configuration['blob']+"/"+plots_file,
                            self.plot_configuration['bucket'],
                            self.plot_configuration['project'],
                            self.plot_credentials )


class TeamsAdaptiveCardStockHighChannel(TeamsAdaptiveCardChannel):

    def __init__(self, name, config, plot_configuration, plot_credentials):
        super().__init__(name, config, plot_configuration, plot_credentials)

    def send(self, params):
        self.plot({
            'table': params['plot_table'],
            'dataframe_grouped_indexed': params['plot_dataframe_grouped_indexed']
        })

        js = self._parametrized_json.format(STOCK_SOURCE=params['STOCK_SOURCE'],
                                            GRAPHICS=params['GRAPHICS'] + "_stockhigh.png",
                                            COLUMN_SETS=params['COLUMN_SETS'])

        super().send(js)

    def plot(self, params):
        TMP_FILES_PATH = "/tmp/"
        pio.templates.default = "plotly_white"
        plt.style.use('fivethirtyeight')
        plt.rcParams['lines.linewidth'] = 1.5
        pd.options.display.max_rows = 100000
        plots_file = str(round(params['table']['timestamp'][0].timestamp())) + str(params['table']['country_iso'][0]) + "_stockhigh.png"
        plot_file_local_system = TMP_FILES_PATH+str(round(params['table']['timestamp'][0].timestamp())) + str(params['table']['country_iso'][0]) + "_stockhigh.png"

        fig = plt.figure(figsize=(12, 6))
        x = params['dataframe_grouped_indexed'][params['dataframe_grouped_indexed']['name_store'] == params['table']['name_store'][0]]['timestamp']
        y = params['dataframe_grouped_indexed'][params['dataframe_grouped_indexed']['name_store'] == params['table']['name_store'][0]]['qty']
        col = 'red'

        x1 = pd.Series(x.iloc[-1])
        y1 = pd.Series(y.iloc[-1])

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                cliponaxis=False,
                line=dict(
                    color='black',
                    width=3.5
                )
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x1,
                y=y1,
                marker=dict(
                    color=col,
                    size=13
                )
            )
        )
        fig.update_layout(height=300, width=500, showlegend=False)
        fig.update_yaxes(range=[0, y.max() * 1.02])
        fig.write_image(plot_file_local_system)

        # upload to azure storage
        upload_plot_google( plot_file_local_system,
                            self.plot_configuration['blob']+"/"+plots_file,
                            self.plot_configuration['bucket'],
                            self.plot_configuration['project'],
                            self.plot_credentials )