from unittest import TestCase, main

from src.exceptions import DeltaReadingError
from src.sparkInit import start_spark
from src.bronze import Bronze
from src.silver import Silver
from src.gold import Gold
import os
import shutil

from src.utils import Schema, Writer


class TestGold(TestCase):
    def setUp(self):
        self.spark = start_spark()
        Bronze.process(self.spark)
        Silver.process(self.spark)

    def tearDown(self):
        dir_path = "data"
        self.spark.stop()
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            shutil.rmtree(dir_path)

    def test_read_dataframes(self):
        with self.assertRaises(DeltaReadingError):
            Silver.read_dataframes('a', Schema.CLIENTS, self.spark)
        df_clients = Gold.read_dataframes(Writer.CLIENTS.format("silver"), Schema.CLIENTS, self.spark)
        self.assertEqual(7, df_clients.count())

    def test_calculate_total_price(self):
        df_products = Gold.read_dataframes(Writer.PRODUCTS.format("bronze"), Schema.PRODUCTS, self.spark)
        df_transactions = Gold.read_dataframes(Writer.TRANSACTIONS.format("bronze"), Schema.TRANSACTIONS, self.spark)
        df = Gold.calculate_total_price(df_transactions, df_products)
        for row in df.collect():
            self.assertEqual(row.Amount * row.Price, row.totalPrice)

    def test_aggregate_per_client(self):
        df_products = Gold.read_dataframes(Writer.PRODUCTS.format("bronze"), Schema.PRODUCTS, self.spark)
        df_transactions = Gold.read_dataframes(Writer.TRANSACTIONS.format("bronze"), Schema.TRANSACTIONS, self.spark)
        df = Gold.calculate_total_price(df_transactions, df_products)
        final_df = Gold.aggregate_per_client(df)
        amount = 0
        mapper = {}
        for client in df.where("Client_ID = 1").collect():
            amount += client.totalPrice
            if client.Product_ID in mapper.keys():
                mapper[client.Product_ID] += client.Amount
            else:
                mapper[client.Product_ID] = client.Amount
        final_row = final_df.where("Client_ID = 1").collect()[0]
        self.assertEqual(amount, final_row.totalPrice)
        self.assertDictEqual(mapper, final_row.transactions)

    def test_append_client_info_and_result(self):
        df_products = Gold.read_dataframes(Writer.PRODUCTS.format("bronze"), Schema.PRODUCTS, self.spark)
        df_clients = Gold.read_dataframes(Writer.CLIENTS.format("silver"), Schema.CLIENTS, self.spark)
        df_transactions = Gold.read_dataframes(Writer.TRANSACTIONS.format("bronze"), Schema.TRANSACTIONS, self.spark)
        df = Gold.calculate_total_price(df_transactions, df_products)
        df_agg = Gold.aggregate_per_client(df)
        final_df = Gold.append_client_info_and_result(df_agg, df_clients)
        for row in final_df.collect():
            if row.totalPrice > row.Income:
                self.assertEqual(True, row.checkDebts)
            else:
                self.assertEqual(False, row.checkDebts)

    def test_process(self):
        Gold.process(self.spark)
        df_final = self.spark.read.format("delta").load(Writer.FINAL)
        self.assertEqual(7, df_final.count())
        df_products = self.spark.read.format("delta").load(Writer.TRANSACTIONS.format("bronze"))
        for row in df_products.collect():
            self.assertEqual(True, row.processed)


if __name__ == "__main__":
    main()
