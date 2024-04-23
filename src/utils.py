from pyspark.sql import types as t


# Creating a class to show where to read files
class Reader:
    CLIENTS = "resources/clients.csv"
    PRODUCTS = "resources/products.csv"
    TRANSACTIONS = "resources/transactions.csv"


# Creating a class to put the schema
class Schema:
    CLIENTS = t.StructType() \
        .add("Client_ID", t.IntegerType()) \
        .add("Name", t.StringType()) \
        .add("Gender", t.StringType()) \
        .add("Income", t.IntegerType())
    PRODUCTS = t.StructType() \
        .add("Product_ID", t.IntegerType()) \
        .add("Name", t.StringType()) \
        .add("Price", t.DoubleType())
    TRANSACTIONS = t.StructType() \
        .add("Client_ID", t.IntegerType()) \
        .add("Product_ID", t.IntegerType()) \
        .add("Amount", t.IntegerType()) \
        .add("Timestamp", t.TimestampType())


# Creating a class in where to save the files
class Writer:
    CLIENTS = "data/{}/clients"
    PRODUCTS = "data/{}/products"
    TRANSACTIONS = "data/{}/transactions"
    FINAL = "data/gold/final"

class Condition:
    CLIENTS = "s.Client_ID = t.Client_ID"
    PRODUCTS = "s.Product_ID = t.Product_ID"
    TRANSACTIONS = ("s.Client_ID = t.Client_ID and s.Product_ID = t.Product_ID and "
                    "s.Amount = t.Amount and s.Timestamp = t.Timestamp")