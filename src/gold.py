# Importing required dependencies
from typing import Union
from src.tables import Delta
from src.utils import Schema, Writer, Condition
from pyspark.sql import types as t, functions as f
from pyspark.sql import DataFrame, SparkSession


# Defining bronze class
class Gold(Delta):
    @staticmethod
    def read_dataframes(path: str, schema: Union[t.StructType, str], spark: SparkSession) -> DataFrame:
        return spark.read.format("delta").load(path)

    @staticmethod
    def calculate_total_price(transaction: DataFrame, products: DataFrame) -> DataFrame:
        """
        Function that joins dataframes for data calculation
        :param transaction: the dataframe that contains the transactions
        :param products: the dataframe that contains the products
        :return: dataframe joined with the total price calculated
        """
        df_joined = transaction.join(products, "Product_ID", "left")
        return df_joined.withColumn("totalPrice", f.col("Amount") * f.col("Price"))

    @staticmethod
    def aggregate_per_client(df: DataFrame) -> DataFrame:
        """
        Function that group the info by client, sum the total amount and map the transactions
        :param df: the dataframe the aggregation will be performed
        :return: Dataframe ready to add client information
        """
        df_client_grouped = df.groupBy("Client_ID", "Product_ID").agg(
            f.sum("Amount").alias("amounts"),
            f.sum("totalPrice").alias("totalPrice"))
        df_mapped = df_client_grouped.groupBy("Client_ID").agg(
            f.map_from_arrays(f.collect_list("Product_ID"), f.collect_list("amounts")).alias("transactions"),
            f.sum("totalPrice").alias("totalPrice")
        )
        return df_mapped

    @staticmethod
    def append_client_info_and_result(df_agg: DataFrame, df_clients: DataFrame) -> DataFrame:
        """
        Function that fill with client data and result the final dataframe
        :param df_agg: the df with the transactions mapped per client and total price
        :param df_clients: the df with the clients information
        :return: the df which will be displayed
        """
        join_df = df_agg.join(df_clients, on="Client_ID", how="inner")
        return join_df.withColumn(
            "checkDebts",
            f.when(f.col("totalPrice") > f.col("income"), True).otherwise(False)) \
            .withColumn("dateCalculated", f.current_date())

    @staticmethod
    def process(spark: SparkSession) -> None:
        # Reading the data
        df_transactions = Gold.read_dataframes(Writer.TRANSACTIONS.format("bronze"), Schema.TRANSACTIONS, spark)
        # Checking if transactions has new data
        if Gold.check_empty(df_transactions.where(f.col("processed") == False)):
            print("no transactions to process")
            return
        df_clients = Gold.read_dataframes(Writer.CLIENTS.format("silver"), Schema.CLIENTS, spark)
        df_products = Gold.read_dataframes(Writer.PRODUCTS.format("bronze"), Schema.PRODUCTS, spark)
        # Calculating total price per transaction
        df_total_price = Gold.calculate_total_price(df_transactions, df_products)
        # Calculating total price per user
        df_agg = Gold.aggregate_per_client(df_total_price)
        final_df = Gold.append_client_info_and_result(df_agg, df_clients)
        # Saving table
        Gold.save_into_delta(Writer.FINAL, final_df, spark, overwrite=True)
        # Upsert transactions because we only need to reprocess in case of new transactions
        Gold.upsert(Writer.TRANSACTIONS.format("bronze"), df_transactions, spark, Condition.TRANSACTIONS)
