# Importing required dependencies
from typing import Union
import delta
from src.tables import Delta
from src.utils import Schema, Writer
from pyspark.sql import types as t, functions as f
from pyspark.sql import DataFrame, SparkSession


# Defining bronze class
class Silver(Delta):
    @staticmethod
    def read_dataframes(path: str, schema: Union[t.StructType, str], spark: SparkSession) -> DataFrame:
        return spark.read.format("delta").schema(schema).load(path)

    @staticmethod
    def filter_data(df: DataFrame) -> DataFrame:
        """
        Function that adds common columns from bronze table
        :param df: dataframe to be saved
        :return: dataframe with columns created
        """
        return df.where(f.col("processed") is False).drop("processed", "creation_date")

    @staticmethod
    def upsert_bronze(df: DataFrame, path: str, condition: str) -> None:
        """
        upsert bronze to set processed as true
        :param df: data
        :param path: the delta table bronze path
        :param condition: the condition to match, use s and t as source and target
        :return: None
        """
        delta.DeltaTable.forPath(path).alias("s").merge(df, condition) \
            .whenMatchedUpdate(set={"processed": "true"})

    @staticmethod
    def process(spark: SparkSession) -> None:
        # instantiating delta object
        # Reading the data
        df_clients = Silver.read_dataframes(Writer.CLIENTS.format("bronze"), Schema.CLIENTS, spark)
        df_products = Silver.read_dataframes(Writer.TRANSACTIONS.format("bronze"), Schema.PRODUCTS, spark)
        df_transactions = Silver.read_dataframes(Writer.PRODUCTS.format("bronze"), Schema.TRANSACTIONS, spark)
        # Inserting the columns
        df_clients = Silver.filter_data(df_clients)
        df_products = Silver.filter_data(df_products)
        df_transactions = Silver.filter_data(df_transactions)
        # Saving the databases
        Silver.save_into_delta(Writer.CLIENTS.format("silver"), df_clients, spark,
                               "s.Client_ID = t.Client_ID")
        Silver.save_into_delta(Writer.TRANSACTIONS.format("silver"), df_transactions, spark)
        Silver.save_into_delta(Writer.PRODUCTS.format("silver"), df_products, spark,
                               "s.Product_ID = t.Product_ID")
