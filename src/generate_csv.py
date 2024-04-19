import random
import pandas as pd
from datetime import datetime, timedelta


# Function to generate random data for clients
def generate_clients(num_clients):
    clients = []
    for i in range(num_clients):
        client_id = i + 1
        name = f"Client_{client_id}"
        gender = random.choice(['Male', 'Female', 'Undefined'])
        income = random.choice([10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000, -1])
        clients.append((client_id, name, gender, income))
    return clients


# Function to generate random data for products
def generate_products(num_products):
    products = []
    for i in range(num_products):
        product_id = i + 1
        name = f"Product_{product_id}"
        price = round(random.uniform(10.0, 1000.0), 2)
        products.append((product_id, name, price))
    return products


# Function to generate random data for transactions
def generate_transactions(num_transactions, num_clients, num_products):
    transactions = []
    for i in range(num_transactions):
        client_id = random.randint(1, num_clients)
        product_id = random.randint(1, num_products)
        amount = random.randint(1, 10)
        timestamp = datetime.now() - timedelta(days=random.randint(1, 365))
        transactions.append((client_id, product_id, amount, timestamp))
    return transactions


# Generate data for clients, products, and transactions
clients_data = generate_clients(10000)
products_data = generate_products(200)
transactions_data = generate_transactions(100000, len(clients_data), len(products_data))

# Create DataFrames
clients_df = pd.DataFrame(clients_data, columns=['Client_ID', 'Name', 'Gender', 'Income'])
products_df = pd.DataFrame(products_data, columns=['Product_ID', 'Name', 'Price'])
transactions_df = pd.DataFrame(transactions_data, columns=['Client_ID', 'Product_ID', 'Amount', 'Timestamp'])

# Save DataFrames to CSV files
clients_df.to_csv('resources/clients.csv', index=False)
products_df.to_csv('resources/products.csv', index=False)
transactions_df.to_csv('resources/transactions.csv', index=False)
