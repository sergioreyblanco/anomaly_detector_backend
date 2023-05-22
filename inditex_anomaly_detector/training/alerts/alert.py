from datetime import datetime
from inditex_anomaly_detector.utils.logging_debugging import common_print


class Alert:

    def __init__(self, name, channel_name, stream_name, model_name,
                 config, internal_source, anomalies_table, anomalies_schema, notification_type, direction, parameters,
                 channel=None, stream=None, model=None):
        self._name = name
        self._channel_name = channel_name
        self._stream_name = stream_name
        self._model_name = model_name

        self._channel = channel
        self._stream = stream
        self._model = model
        self._direction = direction
        self._parameters = parameters
        self._anomalies_table = anomalies_table
        self._anomalies_schema = anomalies_schema
        self._internal_source = internal_source
        self._notification_type = notification_type
        self._create_anomalies_table()

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name=name

    @property
    def stream(self):
        return self._stream

    @stream.setter
    def stream(self, stream):
        self._stream=stream

    @property
    def model(self):
        return self._model

    @model.setter
    def model(self, model):
        self._model=model

    @property
    def channel(self):
        return self._channel

    @channel.setter
    def channel(self, channel):
        self._channel = channel

    @property
    def stream_name(self):
        return self._stream_name

    @property
    def model_name(self):
        return self._model_name

    @property
    def channel_name(self):
        return self._channel_name

    @property
    def direction(self):
        return self._direction

    @property
    def parameters(self):
        return self._parameters

    def _insert_anomaly(self, anomaly_group_id, args, anomaly_type):

        query = "INSERT INTO {} (" + \
                "ALERT_NAME, " + \
                "STREAM_NAME, " + \
                "CHANNEL_NAME, " + \
                "MODEL_NAME, " + \
                "ALERT_TYPE, " + \
                "ANOMALY_GROUP, " + \
                "ANOMALY_TIMESTAMP, " + \
                "NOW_TIMESTAMP, " + \
                "DIMENSIONS, " + \
                "MEASURES, " + \
                "NOTIFICATION_TYPE, " + \
                "OTHER ) " + \
                "SELECT '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'," + \
                "       to_variant(parse_json('{}')), to_variant(parse_json('{}')), '{}', to_variant(parse_json('{}'))"
        query = query.format(
            self._anomalies_table,
            self._name,
            self._stream.name,
            self._channel.name,
            self._model.name,
            self.__class__.__name__,
            str(anomaly_group_id),
            str(args['timestamp']),
            str(datetime.now()),
            str(args['dims']),
            str(args['measures']),
            anomaly_type,
            args['other_data']
        )
        common_print("insert_anomaly query", query)
        self._internal_source.insert(query)


    def alert(self, args):

        # obtener timestamp datapoint actual
        query = "select max(timestamp) as tstamp from {}".format(args['stream_table'])
        df = self._internal_source.fetch(query, 1)
        current_datapoint_timestamp = df['TSTAMP'].iloc[0]
        common_print("current datapoint timestamp", str(current_datapoint_timestamp))

        # obtener timestamp datapoint previo
        query = "select max(timestamp) as tstamp from {} where timestamp != '{}'".format(args['stream_table'], current_datapoint_timestamp)
        df = self._internal_source.fetch(query, 1)
        previous_datapoint_timestamp = df['TSTAMP'].iloc[0]
        common_print("previous datapoint timestamp", str(previous_datapoint_timestamp))

        # comprobar si anomalia en datapoint previo
        query = "select ANOMALY_GROUP as ANOMALY_GROUP, NOTIFICATION_TYPE AS NOTIFICATION_TYPE, ANOMALY_TIMESTAMP AS ANOMALY_TIMESTAMP " + \
                "from {} " + \
                "where alert_name='{}' and stream_name='{}' and channel_name='{}' and model_name='{}' {} " + \
                "order by NOW_TIMESTAMP desc " + \
                "limit 1"
        query = query.format(
            self._anomalies_table,
            self._name,
            self._stream.name,
            self._channel.name,
            self._model.name,
            args['time_series_filter'])
        common_print("check if anomaly in previous datapoint", query)
        df1 = self._internal_source.fetch(query, 1)

        if args['is_anomaly'] == True:
            # si hay anomalia
            args['timestamp'] = current_datapoint_timestamp

            query = "select max(ANOMALY_GROUP) as ANOMALY_GROUP  from {} "
            query = query.format(self._anomalies_table)
            common_print("get anomaly group", query)
            df2 = self._internal_source.fetch(query, 1)
            v = df2['ANOMALY_GROUP'].iloc[0]
            if v == None:
                v = 0
            if len(df1) > 0:
                at=df1['ANOMALY_TIMESTAMP'].iloc[0]
            else:
                at=None

            # si no hay anomalia en datapoints previos
            if at != current_datapoint_timestamp:
                flag_alert=True
                anomaly_group_id = v+1
            # si la anomalia que hay es de tipo close
            elif df1['NOTIFICATION_TYPE'].iloc[0] == 'close':
                flag_alert = True
                anomaly_group_id = v
            # si hay anomalia open en el datapoint previo
            else:
                anomaly_group_id = df1['ANOMALY_GROUP'].iloc[0]
                flag_alert = False

            #envio de la anomalia
            if flag_alert == True or args['force_anomaly'] == True:
                # envio de alert open
                if self._notification_type in ('open','open_close'):
                    self._channel.send(self._alert_message('open', args))

            # insertar nueva anomalia open
            if anomaly_group_id==None:
                anomaly_group_id=0
            self._insert_anomaly(anomaly_group_id, args, 'open')

        else:
            # si no hay anomalia en datapoint actual TODO
            if len(df1) > 0:
                # si hay anomalia en datapoint previo
                if df1['NOTIFICATION_TYPE'].iloc[0] == 'open':
                    args['timestamp'] = previous_datapoint_timestamp

                    # envio de alert close
                    if self._notification_type in ('close', 'open_close'):
                        self._channel.send(self._alert_message('close', args))

                    # insertar nueva anomalia close
                    anomaly_group_id = df1['ANOMALY_GROUP'].iloc[0]
                    if anomaly_group_id == None:
                        anomaly_group_id = 0
                    self._insert_anomaly(anomaly_group_id, args, 'close')


    def _alert_message(self, open, args):
        pass

    def _create_anomalies_table(self):
        self._internal_source.create_if_exists(self._anomalies_table,
                                               self._anomalies_schema)



class ThresholdAlert(Alert):

    def __init__(self, name, channel_name, stream_name, model_name,
                 config, internal_source, anomalies_table, anomalies_schema, notification_type, direction, parameters,
                 channel=None, stream=None, model=None):
        super().__init__(name, channel_name, stream_name, model_name,
                 config, internal_source, anomalies_table, anomalies_schema, notification_type, direction, parameters,
                 channel=None, stream=None, model=None)

    def _alert_message(self, open, args):
        return "[ Anomaly "+open+" ] {}: Value {} exceeded threshold {} on timestamp: {}, stream: {}, time serie: {}".format(self._name, args['value'], args['threshold'], args['timestamp'], args['stream'], args['time_serie'])




class CustomAlert(Alert):

    def __init__(self, name, channel_name, stream_name, model_name,
                 config, internal_source, anomalies_table, anomalies_schema, notification_type, direction, parameters,
                 channel=None, stream=None, model=None):
        super().__init__(name, channel_name, stream_name, model_name,
                 config, internal_source, anomalies_table, anomalies_schema, notification_type, direction, parameters,
                 channel=None, stream=None, model=None)

    #def _alert_message(self, open, args):
    #    return "[ Anomaly "+open+" ] {}: On timestamp: {}, stream: {}, time series: {}".format(self._name, args['timestamp'], args['stream'], args['time_serie'])
    def _alert_message(self, open, args):
        return {'open_close': open,**args}





class ZscoreAlert(Alert):

    def __init__(self, name, channel_name, stream_name, model_name,
                 config, internal_source, anomalies_table, anomalies_schema, notification_type, direction, parameters,
                 channel=None, stream=None, model=None):
        super().__init__(name, channel_name, stream_name, model_name,
                 config, internal_source, anomalies_table, anomalies_schema, notification_type, direction, parameters,
                 channel=None, stream=None, model=None)


    def alert(self, args):
        common_print("zscore", args['z_score_indicador'])
        common_print("zscore_param", self.parameters['z_score_indicador'])
        if all([True if int(float(i)) in self.parameters['z_score_indicador'] else False for i in args['z_score_indicador']]):
            super().alert(args)

    def _alert_message(self, open, args):
        return {'open_close': open,**args}