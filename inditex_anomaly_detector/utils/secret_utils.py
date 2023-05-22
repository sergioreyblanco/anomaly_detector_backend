from pyspark.sql import SparkSession


def get_dbutils():
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(SparkSession.builder.getOrCreate())
    except:
        raise Exception("Unable to import dbutils")
