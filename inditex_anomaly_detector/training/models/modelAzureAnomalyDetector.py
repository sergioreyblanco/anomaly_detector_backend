from inditex_anomaly_detector.training.models.model import Model
from inditex_anomaly_detector.utils.logging_debugging import common_print
from inditex_anomaly_detector.utils.logging_debugging import start_chrono, end_chrono
from azure.ai.anomalydetector import AnomalyDetectorClient
from azure.ai.anomalydetector.models import DetectionRequest, ModelInfo
from azure.ai.anomalydetector.models import ModelStatus, DetectionStatus
from azure.core.credentials import AzureKeyCredential
from zcomcule_ml_commons.connector import AzurestorageConnectorPandas
from inditex_anomaly_detector.utils.secret_utils import get_dbutils
from datetime import datetime
import pandas as pd
import time



class AzureAnomalyDetectorModel(Model):

    def __init__(self, name, parameters, tables, credentials, storage_credentials, other, internal_source, model_table, model_schema):
        super().__init__("anomaly_detector_model", parameters, tables, credentials, storage_credentials,
                         other, internal_source, model_table, model_schema)

        self.ad_client = self.__get_anomaly_detector_client(credentials)
        self.azs = AzurestorageConnectorPandas(**storage_credentials)
        common_print("",str(storage_credentials))
        self._azure_storage_base_url = self.__get_base_url(storage_credentials)
        self._model_container = other["model_container"]
        self._train_container = other["train_container"]


    def __get_anomaly_detector_client(self, credentials):
        keyvault = credentials.get("keyvault")
        key = credentials["credentials"]
        if keyvault:
            key = get_dbutils().secrets.get(keyvault, key)
        return AnomalyDetectorClient(AzureKeyCredential(key), credentials["endpoint"])

    def __get_base_url(self, credentials):
        keyvault = credentials.get("keyvault")
        res = credentials["user"]
        url = credentials["url"]
        if keyvault:
            res = get_dbutils().secrets.get(keyvault, res)
            url = get_dbutils().secrets.get(keyvault, url)
        return "https://{}.blob.{}".format(res, url)

    def __load_az_anomdetect(self, stream, dims, last=None, date=datetime.now()):
        df = stream.get_hist(self._build_query_lastdatatpoints_timeseries(dims, last)).reset_index(drop=True)

        if not last and len(df) < self.parameters['dataset_length_warn']:
            common_print("","Provided dataset for Stream: {} - Dimensions: [{}] may be too small ({} rows)".format(
                stream.name, ', '.join(dims.values()), len(df)))

        df_timestamps = pd.to_datetime(df['TIMESTAMP'])
        start_time = df_timestamps.min().to_pydatetime()
        end_time = df_timestamps.max().to_pydatetime()
        dimensions = self._serialize_time_serie_dims(dims)

        df.columns = df.columns.str.lower()
        df = df.sort_values(by=['timestamp'], ascending=True)


        file_name = '{}_{}_{}.csv.zip'.format(
            stream.name, dimensions, date.strftime('%Y%m%d%H%M%S'))

        file_path = "{}/{}".format(self._train_container, file_name)
        self.azs.write_zip(df, file_path, "overwrite", 5000000)

        url = "{}/{}".format(self._azure_storage_base_url, file_path)

        return url, file_path, start_time, end_time, date

    def train(self, stream, alert):
        start=start_chrono()

        time_series = stream.get_time_series()
        for dims in time_series.to_dict('records'):
            self.__train_time_serie(stream, alert, dims)

        end_chrono("Model train", start)


    def __train_time_serie(self, stream, alert, dims):
        url, file_path, start_time, end_time, train_date = self.__load_az_anomdetect(
            stream, dims)

        data_feed = ModelInfo(start_time=start_time,
                              end_time=end_time, source=url)
        response_header = \
            self.ad_client.train_multivariate_model(
                data_feed, cls=lambda *args: [args[i] for i in range(len(args))])[-1]

        trained_model_id = response_header['Location'].split("/")[-1]

        common_print("","ModelId for Stream: {} - Dimensions: [{}] id {}:".format(
            stream.name, ', '.join(dims.values()), str(trained_model_id)))

        model_status = None

        while model_status != ModelStatus.READY and model_status != ModelStatus.FAILED:
            model_info = self.ad_client.get_multivariate_model(
                trained_model_id).model_info
            model_status = model_info.status
            time.sleep(10)

        self.azs.delete(file_path)

        if model_status == ModelStatus.FAILED:
            common_print("", "Creating model for Stream: {} - Dimensions: [{}] failed, errors:".format(
                stream.name, ', '.join(dims.values())))

            if model_info.errors:
                for error in model_info.errors:
                    common_print("","Error code: {}. Message: {}".format(
                        error.code, error.message))
            else:
                common_print("","None")

        if model_status == ModelStatus.READY:
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

        url, file_path, start_time, end_time, _ = self.__load_az_anomdetect(
            stream, dims, last=self.parameters['last_detect_points'], date=train_date)

        #logging.info(url)
        #logging.info(start_time)
        #logging.info(end_time)
        detection_req = DetectionRequest(source=url, start_time=start_time, end_time=end_time)
        #logging.info(model_id)
        response_header = self.ad_client.detect_anomaly(model_id, detection_req,
                                                        cls=lambda *args: [args[i] for i in range(len(args))])[-1]
        result_id = response_header['Location'].split("/")[-1]

        r = self.ad_client.get_detection_result(result_id)

        #logging.info(r.summary.status)
        while r.summary.status != DetectionStatus.READY and r.summary.status != DetectionStatus.FAILED:
            r = self.ad_client.get_detection_result(result_id)
            time.sleep(1)

        self.azs.delete(file_path)
        #logging.info(model_id)

        if r.summary.status == DetectionStatus.FAILED:
            common_print("","Detection for Stream: {} - Dimensions: [{}]  failed, errors:".format(
                stream.name, ', '.join(dims.values())))
            if r.summary.errors:
                for error in r.summary.errors:
                    common_print("","Error code: {}. Message: {}".format(
                        error.code, error.message))
            else:
                common_print("","None")
            return None

        alert_base = {'stream': stream.name,
                      'stream_table': stream.get_table_name(),
                      'dims': '\''+'\', \''.join(d for d in dims.values())+'\'',
                      'measures': '\''+'\', \''.join(d for d in stream.columns.measures.values())+'\'',
                      'other_data': '\"key3\": \"value3\", \"key4\": \"value4\"'}

        common_print("",alert_base)

        flag_is_anomaly = True
        common_print("", str(r.as_dict()))
        for res in r.as_dict()['results']:
            if (res['value'].get('is_anomaly', False)):
                common_print("",res['timestamp'])
                flag_is_anomaly=False
                self._trigger_alerts({'is_anomaly': flag_is_anomaly, 'timestamp': res['timestamp'], **alert_base})

        if flag_is_anomaly==True:
            self._trigger_alerts({'is_anomaly': flag_is_anomaly, 'timestamp': None, **alert_base})