import re
from inditex_anomaly_detector.utils.logging_debugging import start_chrono, end_chrono
from inditex_anomaly_detector.utils.logging_debugging import common_print


class Stream:

    def __init__(self, name, source_name, query, columns, frequency, delay, fill_gaps, span, stream_tables, internal_source, source):
        self._name = name
        self._source_name=source_name
        self._internal_source = internal_source
        self._source = source
        self._query = query
        self._columns = columns
        self._frequency = frequency; self._frequency[0]=int(self._frequency[0])
        self._delay = delay; self._delay[0]=int(self._delay[0])
        self._fill_gaps = fill_gaps
        self._span = span
        self._stream_tables = stream_tables

    def get_table_name(self):
        return '{}_{}'.format(self._stream_tables, self._name)

    @property
    def columns(self):
        return self._columns

    @property
    def name(self):
        return self._name

    @property
    def source_name(self):
        return self._source_name

    @source_name.setter
    def source_name(self, source_name):
        self._source_name = source_name

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        self._source = source

    @property
    def internal_source(self):
        return self._internal_source

    @internal_source.setter
    def internal_source(self, internal_source):
        self._internal_source = internal_source

    def get_hist(self):
        pass

    def save_hist(self):
        pass

    def check_hist(self):
        pass


    def check_datapoint(self):
        pass

class StreamColumns:

    def __init__(self, date, measures, dims):
        self._date = date
        self._measures = measures
        self._dims = dims

    @property
    def date(self):
        return self._date

    @property
    def measures(self):
        return self._measures

    @property
    def dims(self):
        return self._dims


class SnowflakeStream(Stream):

    def __init__(self, name, source_name, query, columns, frequency, delay, fill_gaps, span, stream_tables, internal_source, source):
        super().__init__(name, source_name, query, columns, frequency, delay, fill_gaps, span, stream_tables, internal_source, source)


    def __fill_gaps_datapoint(self, query, rows_inserted):
        if rows_inserted>0:
            return

        ds = ','.join(self._columns.dims)
        ms_last_value = 'q.' + ', q.'.join(self._columns.measures)
        ms_fill_gaps = '0 as ' + ', 0 as '.join(self._columns.measures)

        if self._fill_gaps == 'last_value':
            first_query_part="""
                select 
                    timestampadd({}, {}, q.{}),
                    {}, 
                    {}
                from 
                (
                """.format(self._frequency[1], self._frequency[0], self._columns.date, ds, ms_last_value)

        elif self._fill_gaps == 'zeros':
            first_query_part = """
                select 
                    timestampadd({}, {}, q.{}),
                    {}, 
                    {}
                from 
                (
                """.format(self._frequency[1], self._frequency[0], self._columns.date, ds, ms_fill_gaps)
        last_query_part = """
            ) as q"""

        fill_gaps_query = (first_query_part+'{}'+last_query_part).format(query)
        df = self._internal_source.fetch(fill_gaps_query, 1)  # TODO: comentar
        common_print("dataframe of fill_gaps_datapoint", str(df.head(100)))

        insert_query = 'INSERT into {} ({})'.format(self.get_table_name(), fill_gaps_query)
        common_print("fill gaps datapoint query", insert_query)
        self._internal_source.insert(insert_query)


    def __fill_gaps_hist(self, query):
        ds=','.join(self._columns.dims)
        ms_last_value = 'q.' + ', q.'.join(self._columns.measures)
        ms_fill_gaps = '0 as ' + ', 0 as '.join(self._columns.measures)

        if self._fill_gaps == 'last_value':
            first_query_part="""
                select 
                    timestampadd({}, {}, q.{}) as {},
                    {}, 
                    {}
                from 
                (
                """.format(self._frequency[1], self._frequency[0], self._columns.date, self._columns.date, ds, ms_last_value)

        elif self._fill_gaps == 'zeros':
            first_query_part = """
                select 
                    timestampadd({}, {}, q.{}) as {},
                    {}, 
                    {}
                from 
                (
                """.format(self._frequency[1], self._frequency[0], self._columns.date, self._columns.date, ds, ms_fill_gaps)

        last_query_part = """
            ) as q
            qualify timestampdiff({}, q.TIMESTAMP, (lag(q.TIMESTAMP,-1) over (partition by {} order by q.TIMESTAMP))) > {}
            """.format(self._frequency[1], ds, self._frequency[0])

        fill_gaps_query = (first_query_part+'{}'+last_query_part).format(query)

        insert_query = 'INSERT into {} ({})'.format(self.get_table_name(), fill_gaps_query)
        common_print("fill gaps hist query", insert_query)
        self._internal_source.insert(insert_query)

        return



    def get_query(self, span_unit, query_span, frequency_value, frequency_units, delay_units, delay_value, maxtimestamp):
        from_time = 'timestampadd({}, -{}, timestampadd({}, -({}+1), convert_timezone(\'UTC\', current_timestamp)))'.format(
            span_unit, query_span, delay_units, delay_value)
        to_time = 'convert_timezone(\'UTC\', current_timestamp)'

        query = self._query
        result = re.search('{FREQ_START}(.*){FREQ_END}', query)
        if result:
            timestamp = result.group(1)
            date_column = "time_slice({}::TIMESTAMP_NTZ, {}, '{}', 'START')".format(
                timestamp, frequency_value, frequency_units)
            #date_column = "date_trunc({}, {})".format(
            #    self._frequency, timestamp)
            query = query.replace(result.group(0), date_column)

        q=query.format(FROM_TIME=from_time, TO_TIME=to_time, MAX_TIME='\''+str(maxtimestamp)+'\'')

        common_print("get_query query", q)
        return q

    def get_time_series(self):
        table_name = self.get_table_name()
        dim_subqueries = map(lambda d: '(select distinct {} from {})'.format(
            d, table_name), self._columns.dims)
        query = 'select * from {}'.format(' join '.join(dim_subqueries))
        return self._source.fetch(query, 1000000)

    def get_hist(self, query_fn):
        table_name = self.get_table_name()
        query = query_fn(self._columns, table_name)
        return self._internal_source.fetch(query, 1000000)

    def save_hist(self):
        start=start_chrono()

        table_name = self.get_table_name()

        maxtimestamp_query = "select timestampadd({}, -{}, convert_timezone('UTC', current_timestamp)) as maxtimestamp".format(self._span[1], self._span[0])
        maxtimestamp_df = self._internal_source.fetch(maxtimestamp_query, 1)
        maxtimestamp = maxtimestamp_df["MAXTIMESTAMP"].iloc[0]
        common_print("maxtimestamp", maxtimestamp)

        query = self.get_query(self._span[1], self._span[0], self._frequency[0], self._frequency[1], self._delay[1], self._delay[0], maxtimestamp)
        create_query = 'create or replace table {} as ({})'.format(table_name, query)
        common_print("create query", create_query)
        self._internal_source.createas(create_query)

        self.__fill_gaps_hist(query)

        end_chrono("Stream save_hist", start)

    def check_hist(self):
        start = start_chrono()
        table_name = self.get_table_name()
        f=self._internal_source.if_exists(table_name)
        if f==False:
            return False

        q="select count(*) as CONTEO from {}".format(table_name)
        common_print("check query", q)
        df = self._internal_source.fetch(q, 1)
        if float(df['CONTEO'].iloc[0]) > 0:
            return True
        else:
            return False

        end_chrono("Stream check_hist", start)


    def check_datapoint(self):
        start = start_chrono()
        table_name = self.get_table_name()
        f=self._internal_source.if_exists(table_name)
        if f==False:
            return False

        check_query = """
        select
            case
                when (select max(TIMESTAMP) from {STREAM_TABLE}) = time_slice(convert_timezone('UTC', current_timestamp)::TIMESTAMP_NTZ, {TIME_VALUE}, '{TIME_UNITS}', 'START')
                    then 1
                else 0
            end as COUNTING"""
        check_query = check_query.format(
            TIME_VALUE=self._frequency[0],
            TIME_UNITS=self._frequency[1],
            STREAM_TABLE=self.get_table_name()
        )
        common_print("checkquery", check_query)
        df = self._internal_source.fetch(check_query, 1)
        common_print("checkqueryresult", df)
        if float(df['COUNTING'].iloc[0]) == 0:
            return False
        elif float(df['COUNTING'].iloc[0]) == 1:
            return True

        end_chrono("Stream check_datapoint", start)


    def save_datapoint(self):
        start = start_chrono()

        table_name = self.get_table_name()

        maxtimestamp_query = "select max({}) as maxtimestamp from {}".format(self._columns.date, table_name)
        maxtimestamp_df = self._internal_source.fetch(maxtimestamp_query, 1)
        maxtimestamp = maxtimestamp_df["MAXTIMESTAMP"].iloc[0]

        query = self.get_query(self._frequency[1], 1*0, self._frequency[0], self._frequency[1], self._delay[1], self._delay[0], maxtimestamp)
        df_datp = self._internal_source.fetch(query, 1)
        common_print("dataframe of save_datapoint", str(df_datp.head(100)))

        insert_query = 'INSERT into {} ({})'.format(table_name, query)
        rows_inserted = self._internal_source.insert(insert_query)
        common_print("save datapoint rows inserted", str(rows_inserted))

        #query=self.get_query(self._frequency[1], 1*self._frequency[0], self._frequency[0], self._frequency[1], self._delay[1], self._delay[0])
        #self.__fill_gaps_datapoint(query, rows_inserted)

        end_chrono("Stream save_datapoint", start)
