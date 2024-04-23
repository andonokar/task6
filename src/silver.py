# Importing required dependencies
from typing import Union
from src.tables import Delta
from src.utils import Schema, Writer, Condition
from pyspark.sql import types as t, functions as f
from pyspark.sql import DataFrame, SparkSession


# Defining bronze class
class Silver(Delta):
    @staticmethod
    def read_dataframes(path: str, schema: Union[t.StructType, str], spark: SparkSession) -> DataFrame:
        return spark.read.format("delta").load(path)

    @staticmethod
    def filter_data(df: DataFrame) -> DataFrame:
        """
        Function that filters from bronze only not processed columns
        :param df: dataframe to be saved
        :return: dataframe with columns created
        """
        return df.where(f.col("processed") == False)

    @staticmethod
    def process(spark: SparkSession) -> None:
        # Reading the data
        df_clients = Silver.read_dataframes(Writer.CLIENTS.format("bronze"), Schema.CLIENTS, spark)
        # Inserting the columns
        df_clients_ok = Silver.filter_data(df_clients)
        # Checking if clients has new data
        if Silver.check_empty(df_clients_ok):
            print("no clients to process")
            return
        df_clients_ok = df_clients_ok.drop("processed", "creation_date")
        # Filtering clients without income because we can't analyse users without income
        df_clients_right = df_clients_ok.where(f.col("income") != -1)
        df_clients_wrong = df_clients_ok.where(f.col("income") == -1)
        # Saving the databases
        Silver.save_into_delta(Writer.CLIENTS.format("silver"), df_clients_right, spark,
                               Condition.CLIENTS)
        Silver.save_into_delta(Writer.CLIENTS.format("silver_nok"), df_clients_wrong, spark,
                               Condition.CLIENTS)
        # Upsert bronze
        Silver.upsert(Writer.CLIENTS.format("bronze"), df_clients_right, spark,
                      Condition.CLIENTS)
        Silver.upsert(Writer.CLIENTS.format("bronze"), df_clients_wrong, spark,
                      Condition.CLIENTS)
