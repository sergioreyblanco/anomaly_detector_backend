from inditex_anomaly_detector.training.models.model import Model
from inditex_anomaly_detector.utils.logging_debugging import start_chrono, end_chrono, common_print
from inditex_anomaly_detector.utils.format_utils import format_number
import pandas as pd
import pytz
from datetime import datetime
import uuid
import json
import copy


TEAMS_CARD_COLUMN={
        "type": "Container",
        "items": [
            {
                "type": "ColumnSet",
                "columns": [
                    {
                        "type": "Column",
                        "width": "10px",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": "",
                                "wrap": "true",
                                "size": "Large",
                                "horizontalAlignment": "Left",
                                "spacing": "Default",
                                "weight": "Default",
                                "separator": "true"
                            }
                        ]
                    },
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": "**Top Country**",
                                "wrap": "true",
                                "size": "Large",
                                "horizontalAlignment": "Left",
                                "spacing": "Default",
                                "weight": "Default",
                                "separator": "true"
                            }
                        ]
                    },
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": "**% Gap**",
                                "wrap": "true",
                                "size": "Large",
                                "horizontalAlignment": "Left",
                                "spacing": "Default",
                                "weight": "Default",
                                "separator": "true"
                            }
                        ]
                    },
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": "**Qty Gap**",
                                "wrap": "true",
                                "size": "Large",
                                "horizontalAlignment": "Left",
                                "spacing": "Default",
                                "weight": "Default",
                                "separator": "true"
                            }
                        ]
                    }
                ]
            }
        ]
    }

TEAMS_CARD_COLUMNSET="""    {{
        "type": "ColumnSet",
        "columns": [
            {{
                "type": "Column",
                "width": "30px",
                "items": [
                    {{
                        "type": "Image",
                        "url": "{COUNTRY_FLAG}",
                        "size": "Small",
                        "horizontalAlignment": "Left"
                    }}
                ]
            }},
            {{
                "type": "Column",
                "width": "stretch",
                "items": [
                    {{
                        "type": "TextBlock",
                        "text": "{NAME_STORE}",
                        "wrap": "true",
                        "size": "Large",
                        "horizontalAlignment": "Left",
                        "spacing": "Default",
                        "weight": "Default",
                        "separator": "true"
                    }}
                ]
            }},
            {{
                "type": "Column",
                "width": "stretch",
                "items": [
                    {{
                        "type": "TextBlock",
                        "text": "{GAP_PERCENTAGE}",
                        "wrap": "true",
                        "size": "Large",
                        "horizontalAlignment": "Left",
                        "spacing": "Default",
                        "weight": "Default",
                        "separator": "true"
                    }}
                ]
            }},
            {{
                "type": "Column",
                "width": "stretch",
                "items": [
                    {{
                        "type": "TextBlock",
                        "text": "{QTY_GAP}",
                        "wrap": "true",
                        "size": "Large",
                        "horizontalAlignment": "Left",
                        "spacing": "Default",
                        "weight": "Default",
                        "separator": "true"
                    }}
                ]
            }}
        ]
    }}"""


class ZscoretinyModel(Model):

    def __init__(self, name, parameters, tables, credentials, storage_credentials, other, internal_source, model_table, model_schema, stream, delay, train_type):
        super().__init__("zscore_tiny_model", parameters, tables, credentials, storage_credentials,
                         other, internal_source, model_table, model_schema, stream, delay, train_type)



    def train(self, stream):
        start=start_chrono()

        #load data
        df = stream.source.fetch(self.parameters['query_hour_train'], 1000000)
        df.columns = df.columns.str.lower()

        #transformations on timestamp variable
        df['timestamp'] = pd.to_datetime(df['snapshot_id'])
        df['timestamp'] = pd.to_datetime(df.timestamp).dt.tz_convert('UTC')
        df = df.sort_values('timestamp')
        df = df[~df.cod_stock_source.str.contains('PST')]
        df = df[~df.name_store.str.contains('Showroom')]
        df_grouped=df.groupby(['name_store','timestamp','country_iso','online_store_name']).sum()

        #calculate rolling avg
        roll_avg = self.parameters['window_value']
        df_grouped[str(roll_avg)+'day_rolling_avg'] = df_grouped.qty.rolling(roll_avg).mean()
        df_grouped[str(roll_avg)+'day_rolling_std'] = df_grouped.qty.rolling(roll_avg).std()

        #calculate now timestamp
        UTC = pytz.utc
        dt_Utc = datetime.now(UTC)
        hora=dt_Utc.strftime("%Y-%m-%d %H:00:00")

        #select needed columns and filter old dates
        df_grouped_indexed = df_grouped.reset_index()
        df_grouped_indexed = df_grouped_indexed.loc[:,
                             ['name_store', 'timestamp', str(roll_avg)+'day_rolling_avg', str(roll_avg)+'day_rolling_std']]
        df_grouped_indexed.columns = ['NAME_STORE', 'TIMESTAMP','COUNTRY_ISO','ONLINE_STORE_NAME' 'DAY_ROLLING_AVG', 'DAY_ROLLING_STD', 'DAY_EWM','PRESALES_AVG']
        df_grouped_indexed = df_grouped_indexed[df_grouped_indexed['TIMESTAMP'] == hora]

        #materialize coefficients
        schema_model={i.split(' ')[0]: i.split(' ')[1] for i in self.tables['zscore_coefficients']['schema'].split(', ')}
        table_name=self.tables['zscore_coefficients']['name']
        stream.source.create_if_exists(table_name, schema_model)
        stream.source.write(df_grouped_indexed, table_name, "append")

        #save model id
        train_date = str(datetime.now())
        trained_model_id = str(uuid.uuid4())
        self._save_model(trained_model_id, stream, {'all':'all'}, train_date)

        end_chrono("Model train", start)



    def detect(self, stream, alert):
        start = start_chrono()

        # df: get data
        df = stream.get_hist(self._build_query_lastdatatpoints_all())
        df.columns = df.columns.str.lower()

        # df: timestamp as index column TODO optimizar
        df_grouped_indexed = df.reset_index()

        # dataframe: get data
        #dataframe_query = stream.get_query('HOUR', self.parameters['window_value'], 5, 'MINUTE', 'MINUTE', 2)
        dataframe_query = self.parameters['query_hour_detect']
        dataframe = stream.source.fetch(dataframe_query, 100000)
        dataframe.columns = dataframe.columns.str.lower()
        dataframe['timestamp'] = pd.to_datetime(dataframe['snapshot_id'], utc=False)
        dataframe['timestamp'] = pd.to_datetime(dataframe.timestamp).dt.tz_localize(None)
        dataframe = dataframe.sort_values('timestamp')
        dataframe = dataframe[~dataframe.cod_stock_source.str.contains('PST')] #TODO
        dataframe = dataframe[~dataframe.name_store.str.contains('Showroom')]
        dataframe_grouped = dataframe.groupby(['online_store_name', 'timestamp']).sum()
        dataframe_grouped_indexed = dataframe_grouped.reset_index()

        # df_coef: get data
        df_coef = stream.source.fetch(self.parameters['query_coefficients'], 100000)
        df_coef.columns = df_coef.columns.str.lower()


        # tabla: merge df and df_coef
        tabla = pd.merge(df_grouped_indexed, df_coef, on=["online_store_name"])
        tabla = tabla.dropna()
        tabla = tabla.reset_index()

        # tabla: zscore columns creation
        tabla['z_score'] = (tabla['qty'] - tabla['day_rolling_avg']) / tabla['day_rolling_std']
        tabla['mean_gap'] = tabla['qty'] - tabla['day_rolling_avg']

        # tabla: merge dataframe and tabla
        actual_time = dataframe[dataframe['name_store'] == tabla['name_store'].iloc[0]]['timestamp'].unique()[-1]
        ########### AQUI
        last_time = dataframe[dataframe['name_store'] == tabla['name_store'].iloc[0]]['timestamp'].unique()[-2]
        ###########
        data = dataframe_grouped_indexed[dataframe_grouped_indexed['timestamp'] == actual_time].sort_values(by=['sum_available_stock_qty'], ascending=False)
        data_ant = dataframe_grouped_indexed[dataframe_grouped_indexed['timestamp'] == last_time].sort_values(by=['sum_available_stock_qty'], ascending=False)
        data_ant = data_ant.loc[:, ['online_store_name', 'sum_available_stock_qty']]
        data = pd.merge(data, data_ant, on=["online_store_name"], how='outer')
        data['change']=data['sum_available_stock_qty_x']-data['sum_available_stock_qty_y']
        data = data.loc[:, ['online_store_name', 'change']]
        tabla = pd.merge(tabla, data, on=["online_store_name"], how='outer')
        tabla = tabla.dropna()
        tabla = tabla[tabla['online_store_name']!='']
        tabla = tabla.reset_index()
        common_print("COMPARE", tabla.head(100))

        # tabla: create zscore indicador column
        tabla['z_score_indicador'] = pd.Series()
        for i in range(0, len(tabla)):
            if (abs(tabla['change'][i]) > 100000):
                tabla['z_score_indicador'][i] = 4
                tabla['mean_gap'][i] = tabla['change'][i]
            elif (abs(tabla['z_score'][i]) > 4.5 and abs(tabla['mean_gap'][i]) > 150000):
                tabla['z_score_indicador'][i] = 1
            elif (abs(tabla['change'][i]) > 50000):
                tabla['z_score_indicador'][i] = 3
                tabla['mean_gap'][i] = tabla['change'][i]
            elif (abs(tabla['z_score'][i]) > 4.5 and abs(tabla['mean_gap'][i]) < 150001):
                tabla['z_score_indicador'][i] = 3
            else:
                tabla['z_score_indicador'][i] = 0

        # tabla: select some columns and filter some rows
        tabla_filtered = tabla
        tabla_filtered = tabla_filtered.loc[:, ['name_store', 'timestamp_x', 'z_score_indicador']]
        tabla_filtered['timestamp_x'] = tabla_filtered['timestamp_x'].dt.tz_localize('UTC')
        tabla_filtered.columns = ['name_store', 'timestamp', 'z_score_indicador']

        #tabla_anterior = pd.DataFrame(columns=['name_store', 'timestamp', 'z_score_indicador'])
        schema_model = {i.split(' ')[0]: i.split(' ')[1] for i in self.tables['zscore_coefficients_previous']['schema'].split(', ')}
        stream.source.create_if_exists(self.tables['zscore_coefficients_previous']['name']+"_"+alert.name.upper(), schema_model)
        tabla_anterior = stream.source.fetch("select * from "+self.tables['zscore_coefficients_previous']['name']+"_"+alert.name.upper(), 10000)
        if len(tabla_anterior.columns)==0:
            tabla_anterior = pd.DataFrame(columns=['name_store', 'timestamp', 'z_score_indicador'])
        else:
            tabla_anterior.columns = ['name_store', 'timestamp', 'z_score_indicador']
        tabla_anterior = tabla_anterior[tabla_anterior['z_score_indicador'] > 0]
        tabla_anterior = tabla_anterior.loc[:, ['name_store', 'timestamp', 'z_score_indicador']]

        # tabla: merge con tabla anterior
        tabla = pd.merge(tabla, tabla_anterior, on=["name_store"], how='outer')
        tabla = tabla.loc[:,
                ['name_store', 'timestamp_x', 'qty', 'day_rolling_avg', 'day_rolling_std', 'z_score_indicador_x', 'mean_gap', 'country_iso', 'z_score_indicador_y']]
        tabla.columns = ['name_store','timestamp','qty','day_rolling_avg','day_rolling_std','z_score_indicador','mean_gap','country_iso','z_score_indicador_anterior']
        tabla = tabla.reset_index()

        tabla = tabla.loc[:,
                ['online_store_name','name_store','timestamp_x','sum_available_stock_qty','day_rolling_avg','day_rolling_std','z_score_indicador_x','mean_gap','country_iso','z_score_indicador_y']]
        tabla.columns = ['online_store_name','name_store','timestamp','qty','day_rolling_avg','day_rolling_std','z_score_indicador','mean_gap','country_iso','z_score_indicador_anterior']
        tabla = tabla.reset_index()

        # tabla: some calculations
        tabla['stock_sources'] = pd.Series()
        dataframe = dataframe.reset_index()
        #dataframe = dataframe.loc[~dataframe.index.duplicated(), :]
        #tabla = tabla.loc[~tabla.index.duplicated(), :]

        tabla['stock_sources']=pd.Series()
        tabla['stock_sources_list']=pd.Series()
        for i in tabla['index'].unique():
                
            data = dataframe[dataframe['online_store_name'] == tabla['online_store_name'][i]][dataframe['timestamp'] == actual_time].sort_values(by=['sum_available_stock_qty'], ascending=False)
            data_ant = dataframe[dataframe['online_store_name'] == tabla['online_store_name'][i]][dataframe['timestamp'] == last_time].sort_values(by=['sum_available_stock_qty'], ascending=False)
            data_grouped = data.groupby(['cod_stock_source']).sum()
            data_ant_grouped = data_ant.groupby(['cod_stock_source']).sum()
            data = data_grouped.reset_index()
            data_ant = data_ant_grouped.reset_index()
            data = pd.merge(data, data_ant, on=["cod_stock_source"], how='outer')
            data['sum_available_stock_qty_x'] = data['sum_available_stock_qty_x'].fillna(0)
            data['sum_available_stock_qty_y'] = data['sum_available_stock_qty_y'].fillna(0)
            data['change'] = abs(data['sum_available_stock_qty_x'] - data['sum_available_stock_qty_y'])
            data['%change'] = ((data['sum_available_stock_qty_x'] - data['sum_available_stock_qty_y']) / tabla['mean_gap'][i]) * 100
            data=data.sort_values(by = ['change'], ascending=False)
            data=data.reset_index()
            if (len(data[data['%change']>10])):
                data=data[data['%change']>10]
            else: 
                data=data[data.index==0]
            tabla['stock_sources'][i]=data['cod_stock_source'][0]
            tabla['stock_sources_list'][i]=data['cod_stock_source'].tolist()

        common_print("tabla final 1", tabla.head(100))

        # tabla: final
        tabla = tabla[tabla['z_score_indicador'] > 0]
        tabla['z_score_indicador_anterior'] = tabla['z_score_indicador_anterior'].replace({4: 1})
        tabla = tabla[tabla['z_score_indicador_anterior'] != tabla['z_score_indicador']]
        tabla = tabla.sort_values(by=['qty'], ascending=False)
        common_print("tabla final 2", tabla.head(100))

        # trigger alerts
        flag_is_anomaly = False
        force_anomaly = False
        for stock in tabla['stock_sources'].unique():
            #common_print("", stock)
            tabla_fil = tabla[tabla['stock_sources'] == stock]
            tabla_fil = tabla_fil.reset_index()

            columns=copy.deepcopy(TEAMS_CARD_COLUMN)
            country_isos=[]
            name_stores = []
            qtys = []
            timestamps = []
            z_score_indicadores = []
            common_print("tabla_fil", tabla_fil.head(100))
            for i in range(0, min(10, len(tabla_fil))):

                line = TEAMS_CARD_COLUMNSET.format(
                    COUNTRY_FLAG="https://storage.googleapis.com/" + alert.channel.plot_configuration['bucket'] + "/country_flags/" + str((tabla_fil['country_iso'][i]).lower()) + ".png",
                    NAME_STORE=tabla_fil['name_store'][i],
                    GAP_PERCENTAGE=str(round(((tabla_fil['mean_gap'][i])/(tabla_fil['qty'][i]-tabla_fil['mean_gap'][i]))*100,2))+' %',
                    QTY_GAP=str(format_number(tabla_fil['mean_gap'][i], 0))
                )
                line = json.loads(line)
                columns['items'].append(line)

                country_isos.append(tabla_fil['country_iso'][i])
                name_stores.append(tabla_fil['name_store'][i])
                qtys.append(str(tabla_fil['qty'][i]))
                timestamps.append(str(tabla_fil['timestamp'][i]))
                z_score_indicadores.append(str(tabla_fil['z_score_indicador'][i] ))

            flag_is_anomaly = True
            alert_base = {'stream': stream.name,
                          'stream_table': stream.get_table_name(),
                          'time_series_filter': 'and DIMENSIONS:COD_STOCK_SOURCE=\''+str(tabla_fil['stock_sources'][0])+'\''+ \
                                                ' and to_variant(parse_json(\''+ str(country_isos).replace("\'", "\"") +'\'))=DIMENSIONS:COUNTRY_ISO',
                          'time_serie': '\'{\"COUNTRY_ISO\": ' +str(country_isos).replace("\'", "\"") + \
                                        ',\"NAME_STORE\": ' +str(name_stores).replace("\'", "\"") + \
                                        ',\"COD_STOCK_SOURCE\": \"' + str(tabla_fil['stock_sources'][0]) + '\"}',
                          'measures': '{\"QTY\": ' + str(qtys).replace("\'", "\"") + '}',
                          'dims': '{\"COUNTRY_ISO\": ' +str(country_isos).replace("\'", "\"") + \
                                        ',\"NAME_STORE\": ' +str(name_stores).replace("\'", "\"") + \
                                        ',\"COD_STOCK_SOURCE\": \"' + str(tabla_fil['stock_sources'][0]) + '\"}',
                          'other_data': '{\"COD_STOCK_SOURCE\": \"' + str(tabla_fil['stock_sources'][0]) + \
                                        '\", \"TIMESTAMP\": ' +str(timestamps).replace("\'", "\"") + \
                                        ', \"Z_SCORE_INDICADOR\": ' +str(z_score_indicadores).replace("\'", "\"") +\
                                        '}',
                          'z_score_indicador': z_score_indicadores,

                          'GRAPHICS': "https://storage.googleapis.com/" + alert.channel.plot_configuration['bucket'] + "/" + alert.channel.plot_configuration['blob'] + "/" + str(round(tabla_fil['timestamp'][0].timestamp())) + str(tabla_fil['country_iso'][0]),
                          'STOCK_SOURCE':'**AFFECTED STOCK SOURCE**: '+tabla_fil['stock_sources'][0],
                          'COLUMN_SETS':json.dumps(columns, indent = 4),
                          'plot_table': tabla_fil,
                          'plot_dataframe_grouped_indexed': dataframe_grouped_indexed                          }
            common_print("json", json.dumps(columns, indent = 4))

            self.add_alert(alert)
            self._trigger_alert({'is_anomaly': flag_is_anomaly, 'force_anomaly': force_anomaly, 'timestamp': str(datetime.now()), **alert_base})

        tabla_filtered.columns = tabla_filtered.columns.str.upper()
        common_print("coefs previous", tabla_filtered.head(100))
        stream.source.write(tabla_filtered, self.tables['zscore_coefficients_previous']['name']+"_"+alert.name.upper(), 'overwrite')

        end_chrono("Model detect", start)