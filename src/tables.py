from abc import ABC, abstractmethod
from typing import Union
from pyspark.sql import types as t
from pyspark.sql import DataFrame, SparkSession
from delta import DeltaTable
from typing import Optional


# Abstract class
class Delta(ABC):
    # Function that ingest the tables
    @staticmethod
    @abstractmethod
    def read_dataframes(path: str, schema: Union[t.StructType, str], spark: SparkSession) -> DataFrame:
        """
        Function that reads a file with and outputs a dataframe
        :param path: where the file is located
        :param schema: schema of the data
        :param spark: a spark session
        :return: dataframe with the schema provided
        """
        ...

    # Defining function to save/upsert tables
    @staticmethod
    def save_into_delta(path: str, df: DataFrame, spark: SparkSession, condition: Optional[str] = None) -> None:
        """
        Saves dataframe into delta table
        :param path: path of the table
        :param df: dataframe to be used
        :param spark: the sparkSession
        :param condition: condition that determines if upsert or appends, fill with s and t
        :return: None

        Example::

            Delta.save_into_delta("/path/to/table", df, spark, "s.id = t.id")
        """
        if not DeltaTable.isDeltaTable(spark, path):
            df.write.format("delta").mode("overwrite").save(path)
            return
        if condition:
            DeltaTable.forPath(spark, path).alias("s") \
                .merge(df.alias("t"), condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            return
        df.write.format("delta").mode("append").save(path)

    # Function that process the tables
    @staticmethod
    @abstractmethod
    def process(spark: SparkSession) -> None:
        """
        function that leverages the processing and save the delta tables
        :return: None
        """
        ...
