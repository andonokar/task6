# Importing required dependencies
from typing import Union
from src.exceptions import CsvReadingError
from src.tables import Delta
from src.utils import Reader, Schema, Writer, Condition
from pyspark.sql import types as t, functions as f
from pyspark.sql import DataFrame, SparkSession


# Defining bronze class
class Bronze(Delta):
    @staticmethod
    def read_dataframes(path: str, schema: Union[t.StructType, str], spark: SparkSession) -> DataFrame:
        try:
            return spark.read.format("csv").option("header", "true").schema(schema).load(path)
        except Exception as err:
            raise CsvReadingError(err)

    @staticmethod
    def insert_columns(df: DataFrame) -> DataFrame:
        """
        Function that adds common columns from bronze table
        :param df: dataframe to be saved
        :return: dataframe with columns created
        """
        return df.withColumn("processed", f.lit(False)) \
            .withColumn("creation_date", f.current_timestamp())

    @staticmethod
    def process(spark: SparkSession) -> None:
        # Reading the data
        df_clients = Bronze.read_dataframes(Reader.CLIENTS, Schema.CLIENTS, spark)
        df_products = Bronze.read_dataframes(Reader.PRODUCTS, Schema.PRODUCTS, spark)
        df_transactions = Bronze.read_dataframes(Reader.TRANSACTIONS, Schema.TRANSACTIONS, spark)
        # Inserting the columns
        df_clients = Bronze.insert_columns(df_clients)
        df_products = Bronze.insert_columns(df_products)
        df_transactions = Bronze.insert_columns(df_transactions)
        # Saving the databases
        Bronze.save_into_delta(Writer.CLIENTS.format("bronze"), df_clients, spark,
                               Condition.CLIENTS)
        Bronze.save_into_delta(Writer.TRANSACTIONS.format("bronze"), df_transactions, spark,
                               Condition.TRANSACTIONS)
        Bronze.save_into_delta(Writer.PRODUCTS.format("bronze"), df_products, spark,
                               Condition.PRODUCTS)
