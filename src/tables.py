from abc import ABC, abstractmethod
from typing import Union
from pyspark.sql import types as t
from pyspark.sql import DataFrame, SparkSession
from delta import DeltaTable
from typing import Optional

from src.exceptions import DeltaWritingError, DeltaUpsertError


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
        :raises ReadingError: If the path could not be read
        """
        ...

    # Defining function to save/upsert tables
    @staticmethod
    def save_into_delta(path: str, df: DataFrame, spark: SparkSession, condition: Optional[str] = None,
                        overwrite: bool = False) -> None:
        """
        Saves dataframe into delta table
        :param path: path of the table
        :param df: dataframe to be used
        :param spark: the sparkSession
        :param overwrite: if the table should be rewritten every time
        :param condition: condition that determines if upsert or appends, fill with s and t
        :return: None
        :raises Union[DeltaWritingError, DeltaUpsertError]: When creation or upsert fails

        Example::

            Delta.save_into_delta("/path/to/table", df, spark, "s.id = t.id")
        """
        if not DeltaTable.isDeltaTable(spark, path) or overwrite:
            try:
                df.write.format("delta").mode("overwrite").save(path)
            except Exception as err:
                raise DeltaWritingError(err)
            return
        if condition:
            try:
                DeltaTable.forPath(spark, path).alias("s") \
                    .merge(df.alias("t"), condition) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            except Exception as err:
                raise DeltaUpsertError(err)
            return
        try:
            df.write.format("delta").mode("append").save(path)
        except Exception as err:
            raise DeltaWritingError(err)

    # Function that process the tables
    @staticmethod
    @abstractmethod
    def process(spark: SparkSession) -> None:
        """
        function that leverages the processing and save the delta tables
        :return: None
        """
        ...

    @staticmethod
    def upsert(path: str, df: DataFrame, spark: SparkSession, condition: str) -> None:
        """
        upsert to set processed as true
        :param df: data
        :param path: the delta table bronze path
        :param spark: spark session to operate the delta table
        :param condition: the condition to match, use s and t as source and target
        :raises DeltaUpsertError: when condition not match, duplicates or missing processed column
        :return: None

        Example::

            Delta.upsert_bronze("/path/to/table", df, spark, "s.id = t.id")
        """
        try:
            DeltaTable.forPath(spark, path).alias("s").merge(df.alias("t"), condition) \
                .whenMatchedUpdate(set={"processed": "true"}).execute()
        except Exception as err:
            raise DeltaUpsertError(err)

    @staticmethod
    def check_empty(df: DataFrame) -> bool:
        return df.isEmpty()
