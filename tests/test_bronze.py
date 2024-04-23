from unittest import TestCase, main
from src.bronze import Bronze
from src.exceptions import CsvReadingError
from src.sparkInit import start_spark
from src.utils import Schema, Reader, Writer
import os
import shutil


class TestBronze(TestCase):
    def setUp(self):
        self.spark = start_spark()

    def tearDown(self):
        dir_path = "data"
        self.spark.stop()
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            shutil.rmtree(dir_path)

    def test_read_dataframes(self):
        with self.assertRaises(CsvReadingError):
            Bronze.read_dataframes('a', Schema.CLIENTS, self.spark)
        df_clients = Bronze.read_dataframes(Reader.CLIENTS, Schema.CLIENTS, self.spark)
        self.assertEqual(9, df_clients.count())

    def test_insert_columns(self):
        df_clients = Bronze.read_dataframes(Reader.CLIENTS, Schema.CLIENTS, self.spark)
        final_df = Bronze.insert_columns(df_clients)
        self.assertIn("processed", final_df.columns)
        self.assertIn("creation_date", final_df.columns)
        self.assertEqual(False, final_df.collect()[0].processed)

    def test_process(self):
        Bronze.process(self.spark)
        Bronze.process(self.spark)
        df_clients = self.spark.read.format("delta").load(Writer.CLIENTS.format("bronze"))
        df_products = self.spark.read.format("delta").load(Writer.PRODUCTS.format("bronze"))
        df_transactions = self.spark.read.format("delta").load(Writer.TRANSACTIONS.format("bronze"))
        self.assertEqual(9, df_clients.count())
        self.assertEqual(9, df_products.count())
        self.assertEqual(87, df_transactions.count())


if __name__ == "__main__":
    main()
