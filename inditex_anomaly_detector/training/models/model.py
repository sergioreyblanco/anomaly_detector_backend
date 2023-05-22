
from inditex_anomaly_detector.utils.logging_debugging import common_print



class Model:

    def __init__(self, name, parameters, tables, credentials, storage_credentials, other, internal_source, model_table, model_schema, stream, delay, train_type):
        self._name = name
        self._parameters = parameters
        self._tables = tables
        self._model_table = model_table
        self._model_schema = model_schema
        self._internal_source = internal_source
        self._alert = None
        self._create_model_table()
        self.stream = stream
        self._delay = delay
        self._train_type = train_type

    @property
    def name(self):
        return self._name

    @property
    def stream(self):
        return self._stream

    @stream.setter
    def stream(self, stream):
        self._stream=stream

    @property
    def parameters(self):
        return self._parameters


    @property
    def tables(self):
        return self._tables

    def add_alert(self, alert):
        self._alert = alert

    def train(self, stream):
        pass

    def detect(self, stream, alert):
        pass

    def plot(self, data):
        pass


    def _create_model_table(self):
        self._internal_source.create_if_exists(self._model_table,
                                               self._model_schema)

    def _trigger_alert(self, args):
        self._alert.alert(args)

    def _retrieve_model(self, stream, dims):
        table = self._model_table
        #dimension_contains = ["array_contains('{}'::variant, dimension_values)".format(
        #    d) for d in dims.values()] #TODO
        query = "select id, train_date from {} where stream_name = '{}' and model_name = '{}' order by train_date desc limit 1"
        query=query.format(table, stream.name, self.name)
        common_print("retrive model query", query)
        df = self._internal_source.fetch(query, 1)

        if df.empty:
            return None
        else:
            return df.iloc[0][0], df.iloc[0][1]

    def _save_model(self, model_id, stream, dims, train_date):
        table = self._model_table
        dimensions = 'array_construct({})'.format(
            ','.join("'{}'".format(d) for d in dims.values()))
        source_type = stream.source.name
        time_date = train_date
        query = "INSERT INTO {} (id, source_name, stream_name, model_name, train_date) SELECT '{}', '{}', '{}', '{}', '{}'"
        query = query.format(
            table, model_id, source_type, stream.name, self._name, time_date)
        self._internal_source.insert(query)

    def _build_query_lastdatatpoints_timeseries(self, dims, last_datapoints):

        def query_fn(columns, table):
            query = "select to_varchar({}, 'yyyy-mm-ddThh:mi:ssZ') as timestamp, {} as value from {}"
            if dims:
                query += ' where ' + \
                         ' and '.join(["{} = '{}'".format(d, v)
                                       for d, v in dims.items()])

            query += ' order by timestamp desc'

            if last_datapoints:
                query += ' limit {}'.format(last_datapoints)

            return query.format(columns.date, columns.measures, table)

        #common_print("build_query_lastdatatpoints_timeseries query", query_fn)
        return query_fn

    def _build_query_lastdatatpoints_all(self):

        def query_fn(columns, table):
            query = "select distinct * from {} where timestamp=(select max(timestamp) from {})"
            return query.format(table, table)

        return query_fn

    def _serialize_time_serie_dims(self, dims):
        return '_'.join(d for d in dims.values())
