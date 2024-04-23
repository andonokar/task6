from unittest import TestCase, main
from src.bronze import Bronze
from src.silver import Silver
from src.exceptions import DeltaReadingError
from src.sparkInit import start_spark
from src.utils import Schema, Writer
import os
import shutil


class TestSilver(TestCase):
    def setUp(self):
        self.spark = start_spark()
        Bronze.process(self.spark)

    def tearDown(self):
        dir_path = "data"
        self.spark.stop()
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            shutil.rmtree(dir_path)

    def test_read_dataframes(self):
        with self.assertRaises(DeltaReadingError):
            Silver.read_dataframes('a', Schema.CLIENTS, self.spark)
        df_clients = Silver.read_dataframes(Writer.CLIENTS.format("bronze"), Schema.CLIENTS, self.spark)
        self.assertEqual(9, df_clients.count())

    def test_filter_data(self):
        df_clients = Silver.read_dataframes(Writer.CLIENTS.format("bronze"), Schema.CLIENTS, self.spark)
        df_clients = Silver.filter_data(df_clients)
        for clients in df_clients.collect():
            self.assertEqual(False, clients.processed)

    def test_process(self):
        Silver.process(self.spark)
        df_bronze = self.spark.read.format("delta").load(Writer.CLIENTS.format("bronze"))
        for clients in df_bronze.collect():
            self.assertEqual(True, clients.processed)
        df_silver = self.spark.read.format("delta").load(Writer.CLIENTS.format("silver"))
        df_silver_nok = self.spark.read.format("delta").load(Writer.CLIENTS.format("silver_nok"))
        self.assertEqual(7, df_silver.count())
        self.assertEqual(2, df_silver_nok.count())


if __name__ == "__main__":
    main()
