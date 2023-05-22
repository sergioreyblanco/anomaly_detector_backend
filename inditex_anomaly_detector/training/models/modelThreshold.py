
from inditex_anomaly_detector.training.models.model import Model
from inditex_anomaly_detector.utils.logging_debugging import common_print
from inditex_anomaly_detector.utils.logging_debugging import start_chrono, end_chrono
from datetime import datetime
import uuid


class ThresholdModel(Model):

    def __init__(self, name, parameters, tables, credentials, storage_credentials, other, internal_source, model_table, model_schema):
        super().__init__("threshold_model", parameters, tables, credentials, storage_credentials,
                         other, internal_source, model_table, model_schema)



    def train(self, stream, alert):
        start=start_chrono()

        time_series = stream.get_time_series()
        for dims in time_series.to_dict('records'):
            self.__train_time_serie(stream, alert, dims)
        common_print("","No need of training for threshold model")

        end_chrono("Model train", start)


    def __train_time_serie(self, stream, alert, dims):
        train_date = str(datetime.now())
        trained_model_id = str(uuid.uuid4())
        self._save_model(trained_model_id, stream, alert, dims, train_date)

    def detect(self, stream, alert):
        start = start_chrono()

        time_series = stream.get_time_series()
        for dims in time_series.to_dict('records'):
            self.__detect_time_serie(stream, alert, dims)

        end_chrono("Model detect", start)

    def __detect_time_serie(self, stream, alert, dims):
        model = self._retrieve_model(stream, dims, alert)
        if model is None:
            common_print("","No models available for Stream: {} - Dimensions: [{}], skipped".format(
                stream.name, ', '.join(dims.values())))
            return
        model_id, train_date = model

        df = stream.get_hist(self._build_query_lastdatatpoints_timeseries(dims, 1)).reset_index(drop=True)
        common_print("df get_hist threshold model", str(df.head(20)))
        v = df['VALUE'].iloc[0] #v = df[stream.columns.measure].iloc[0]
        common_print("value df get_hist threshold model", str(v))

        alert_base = {'stream': stream.name,
                      'stream_table': stream.get_table_name(),
                      'time_serie': '\'' + '\', \''.join(d for d in dims.values()) + '\'',
                      'threshold': alert.parameters['threshold_value'],
                      'value': v,
                      'measures': stream.columns.measures,
                      'other_data': '\"key3\": \"value3\", \"key4\": \"value4\"'}

        flag_is_anomaly = False
        if alert.direction=='upper' and v > alert.parameters['threshold_value']:
            flag_is_anomaly = True
        elif alert.direction=='lower' and v < alert.parameters['threshold_value']:
            flag_is_anomaly = True

        self._trigger_alerts({'is_anomaly': flag_is_anomaly, 'timestamp': str(datetime.now()), **alert_base})