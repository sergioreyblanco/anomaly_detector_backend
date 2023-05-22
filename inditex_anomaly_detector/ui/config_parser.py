import yaml
from inditex_anomaly_detector.utils.logging_debugging import start_chrono, end_chrono
import os
from inditex_anomaly_detector.training.streams_sources.stream import StreamColumns
from inditex_anomaly_detector.training.models import *
from inditex_anomaly_detector.training.channels import *
from inditex_anomaly_detector.training.streams_sources import *
from inditex_anomaly_detector.training.alerts import *
from inditex_anomaly_detector.utils.logging_debugging import common_print


class ConfigParser:

    def __init__(self, config_file):
        self._config_file = config_file
        self._is_databricks = self.__is_databricks()

    @property
    def is_databricks(self):
        return self._is_databricks

    def __is_databricks(self):
        try:
            from pyspark.dbutils import DBUtils
            return True
        except:
            return False

    def parse(self, name):
        with open(self._config_file, 'r') as reader:
            config = yaml.safe_load(reader)
            return config


class SourceConfigParser(ConfigParser):

    def __init__(self, config_file):
        super().__init__(config_file)
        self._config_file=config_file
        self._sources = {}

    def get_source(self, name):
        try:
            return self._sources[name]
        except KeyError:
            raise Exception('Unknown source name')

    def parse(self, name):
        start=start_chrono()

        config = super().parse(self._config_file)
        config_sources = config['sources']
        config_source = [s for s in config_sources if s['name'] == name][0]
        s=parse_source(self, config_source, self._sources)

        end_chrono("SourceConfig parse", start)

        return s

class StreamConfigParser(ConfigParser):

    def __init__(self, config_file, internal_config_parser):
        super().__init__(config_file)
        self._config_file=config_file
        #self._source_config_parser = source_config_parser
        self._internal_config_parser = internal_config_parser
        self._streams = {}


    def get_stream(self, name):
        try:
            return self._streams[name]
        except KeyError:
            raise Exception('Unknown stream name {}'.format(name))

    def parse(self, name):
        start=start_chrono()
        config = super().parse("")

        config_streams = config['streams']
        config_stream = [s for s in config_streams if s['name'] == name][0]
        s=self.__parse_stream(config_stream)

        end_chrono("StreamConfig parse", start)

        return s


    def __parse_stream(self, config_stream):
        name = config_stream['name']
        #source = self._source_config_parser.get_source(config_stream['source'])
        query = config_stream['query']

        historic_span = config_stream['historic_span'].split(' ')
        frequency = config_stream['frequency'].split(' ')
        delay = config_stream['delay'].split(' ')
        fill_gaps = config_stream['fill_gaps']
        columns = self.__parse_columns(config_stream['columns'])

        source_cp = SourceConfigParser(self._config_file)
        source = source_cp.parse(config_stream['source'])

        str_type = type(source).__name__.replace('Source', '')
        stream = eval(str_type + 'Stream')(name=name, source_name=config_stream['source'], query=query,
                                           columns=columns, frequency=frequency, delay=delay, fill_gaps=fill_gaps, span=historic_span,
                                           stream_tables=self._internal_config_parser.stream_tables,
                                           internal_source=self._internal_config_parser.internal_source, source=source)
        self._streams[name] = stream
        return stream

    def __parse_columns(self, config_columns):
        date = config_columns['date']
        measures = config_columns['measure']
        dims = config_columns['dim']

        return StreamColumns(date, measures, dims)


class ChannelConfigParser(ConfigParser):

    def __init__(self, config_file):
        super().__init__(config_file)
        self._channels = {}
        self._credentials = {}

    def __get_credential(self, name):
        try:
            return self._credentials[name]
        except KeyError:
            raise Exception('Unknown credential name {}'.format(name))

    def __parse_credentials(self, config):
        config_creds = config['credentials']
        for cred in config_creds:
            self._credentials[cred['name']] = get_credentials(self, cred)

    def get_channel(self, name):
        try:
            return self._channels[name]
        except KeyError:
            raise Exception('Unknown channel name {}'.format(name))

    def parse(self, name):
        start=start_chrono()
        config = super().parse("")

        self.__parse_credentials(config)
        config_channels = config['channels']
        config_channel = [c for c in config_channels if c['name'] == name][0]
        c=self.__parse_channel(config_channel)

        end_chrono("channelConfig parse", start)

        return c


    def __parse_channel(self, config_channel):
        name = config_channel['name']
        type = config_channel['type']
        plot_configuration = config_channel.get('plot_configuration')
        try:
            plot_credentials = self.__get_credential(plot_configuration['credentials'])
        except:
            plot_credentials=None

        channel = eval(type + 'Channel')(name, config_channel, plot_configuration, plot_credentials)

        self._channels[name] = channel
        return channel


class ModelConfigParser(ConfigParser):

    def __init__(self, config_file, internal_config_parser):
        super().__init__(config_file)
        self._internal_config_parser = internal_config_parser
        self._models = {}
        self._credentials = {}

    def get_model(self, name):
        try:
            return self._models[name]
        except KeyError:
            raise Exception('Unknown model name {}'.format(name))

    def __get_credential(self, name):
        try:
            return self._credentials[name]
        except KeyError:
            raise Exception('Unknown credential name {}'.format(name))

    def parse(self, name):
        start=start_chrono()
        config = super().parse("")

        config_models = config['models']
        config_model = [m for m in config_models if m['name'] == name][0]
        self.__parse_credentials(config) #todo
        m=self.__parse_model(config_model)

        end_chrono("ModelConfig parse", start)

        return m

    def __parse_credentials(self, config):
        config_creds = config['credentials']
        for cred in config_creds:
            self._credentials[cred['name']] = get_credentials(self, cred)

    def __parse_model(self, config_model):

        name = config_model['name']
        stream = config_model['stream']
        delay = config_model['delay']
        train_type = config_model['train_type']
        parameters = config_model.get('parameters')
        tables = config_model.get('tables')
        type = ''.join(map(lambda token: token.capitalize(), name.split('_')))

        credentials_name = config_model.get('credentials')
        storage_credentials_name = config_model.get('storage_credentials')


        try:
            other = {}
            for line in config_model['other']:
                for k, v in line.items():
                    other[k] = v
        except:
            other = {}
        try:
            credentials = self.__get_credential(credentials_name)
        except:
            credentials=None
        try:
            storage_credentials = self.__get_credential(storage_credentials_name)
        except:
            storage_credentials=None


        model = eval(type + 'Model')(name, parameters, tables, credentials, storage_credentials,
                                     other, self._internal_config_parser.internal_source,
                                     self._internal_config_parser.model_table,
                                     self._internal_config_parser.model_table_schema,
                                     stream, delay, train_type)

        self._models[name] = model
        return model


class AlertConfigParser(ConfigParser):

    def __init__(self, config_file, internal_config_parser):
        super().__init__(config_file)
        self._config_file=config_file
        #self._stream_config_parser = stream_config_parser
        #self._channel_config_parser = channel_config_parser
        #self._model_config_parser = model_config_parser
        self._internal_config_parser = internal_config_parser

    def parse(self, name):
        start=start_chrono()

        config = super().parse(self._config_file)

        config_alerts = config['alerts']
        config_alert = [a for a in config_alerts if a['name']==name][0]
        a=self.__parse_alert(config_alert)

        end_chrono("AlertConfig parse", start)

        return a

    def __parse_alert(self, config_alert):
        name = config_alert['name']
        #stream = self._stream_config_parser.get_stream(config_alert['stream'])
        #channel = self._channel_config_parser.get_channel(config_alert['channel'])
        #model = self._model_config_parser.get_model(config_alert['model'])

        stream_name = config_alert['stream']
        channel_name = config_alert['channel']
        model_name = config_alert['model']

        type = ''.join(map(lambda token: token.capitalize(),
                       config_alert['type'].split('_')))
        alert = eval(type + 'Alert')(name=name, channel_name=channel_name, stream_name=stream_name, model_name=model_name,
                                    config=config_alert, internal_source=self._internal_config_parser.internal_source, anomalies_table=self._internal_config_parser.anomalies_table+"_"+name,
                                    anomalies_schema=self._internal_config_parser.anomalies_table_schema,
                                    notification_type=config_alert['notification_type'], direction=config_alert['direction'], parameters=config_alert['parameters'],
                                    channel=None, stream=None, model=None)

        #model.add_alert(alert)

        return alert


class InternalConfigParser(ConfigParser):

    def __init__(self, config_file):
        super().__init__(config_file)

    @property
    def internal_source(self):
        return self._internal_source

    @property
    def stream_tables(self):
        return self._tables['stream_tables']['name']

    @property
    def model_table(self):
        return self._tables['model_table']['name']

    @property
    def model_table_schema(self):
        return {i.split(' ')[0]: i.split(' ')[1] for i in self._tables['model_table']['schema'].split(', ')}

    @property
    def anomalies_table(self):
        return self._tables['anomalies_table']['name']

    @property
    def anomalies_table_schema(self):
        return {i.split(' ')[0]: i.split(' ')[1] for i in self._tables['anomalies_table']['schema'].split(', ')}

    def parse(self, name):
        start=start_chrono()

        config = super().parse("")
        internal_config = config['internal_source'][0]
        self._internal_source = parse_source(self, internal_config)
        self._tables = {}
        for k, v in config['tables'].items():
            self._tables[k] = v

        end_chrono("InternalConfig parse", start)


def parse_source(parser, config_source, source_dict=None):
    source_config = get_credentials(parser, config_source)
    name = source_config['name']
    type = source_config['type'].capitalize()

    source = eval(type + 'Source')(name, source_config)

    if (source_dict is not None):
        source_dict[name] = source
    return source


def get_credentials(parser, config_source):
    if not parser.is_databricks:
        config_source.pop('keyvault', None)
        for k, v in config_source.items():
            env = os.getenv(v.replace('-', '_'))
            if env:
                config_source[k] = env

    return config_source
