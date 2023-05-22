from zcomcule_ml_commons.connector import *
import pandas as pd
import snowflake.connector
from inditex_anomaly_detector.utils.logging_debugging import common_print

class Source:

    def __init__(self, name, creds_dict=None):
        self._name = name
        self._creds_dict = creds_dict

    @property
    def name(self):
        return self._name


class SnowflakeSource(Source):

    def __init__(self, name, creds_dict=None):
        super().__init__(name, creds_dict)
        self._conector = SnowflakeConnectorPandas(**self._creds_dict)

    def fetch(self, query, size):
        try:
            try:
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession
                dbut=DBUtils(SparkSession.builder.getOrCreate())
            except ImportError:
                common_print("error getting dbutils", "")
            di = {
                'user': dbut.secrets.get(scope=self._creds_dict['keyvault'], key=self._creds_dict['user']),
                'password': dbut.secrets.get(scope=self._creds_dict['keyvault'], key=self._creds_dict['password']),
                'account': dbut.secrets.get(scope=self._creds_dict['keyvault'], key=self._creds_dict['url']),
                'warehouse': self._creds_dict['warehouse'],
                'database': self._creds_dict['database'],
                'schema': self._creds_dict['schema'],
                'role': "R_DIGITALDATA_PIPELINE"
            }
        except:
            di = {
                'user': self._creds_dict['user'],
                'password': self._creds_dict['password'],
                'account': self._creds_dict['url'],
                'warehouse': self._creds_dict['warehouse'],
                'database': self._creds_dict['database'],
                'schema': self._creds_dict['schema'],
                'role': "R_DIGITALDATA_PIPELINE"
            }

        common_print("creds_dict post", di)
        conn = snowflake.connector.connect(**di)
        dataset = pd.DataFrame()
        cs = conn.cursor()
        cs.execute(query)
        while True:
            dat = cs.fetchmany(size)
            dat = pd.DataFrame(dat, columns=[c[0] for c in cs.description])
            if dat.empty:
                break
            dataset = pd.concat([dataset, pd.DataFrame(dat)])

        #return self._conector.read(query, size)
        return dataset

    def write(self, dataframe, table, mode):
        return self._conector.write(dataframe, table, mode, 10000000)

    def createas(self, query):
        self._conector.execute_createas(query)

    def insert(self, query):
        return self._conector.execute_update_insert(query)

    def close(self):
        self._conector.close_connection()

    def create_if_exists(self, table, schema):
        try:
            self._conector.exists(table)
        except:
            self._conector.create(table, schema)

    def if_exists(self, table):
        try:
            self._conector.exists(table)
            return True
        except:
            return False