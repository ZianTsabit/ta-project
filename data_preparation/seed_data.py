from datetime import datetime, timedelta
from faker import Faker

import pymongo
import random

# Establish a connection to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

fake = Faker()

db = client["db_shop"]

offices_coll = db["offices"]
employees_coll = db["employees"]
clients_coll = db["clients"]
payments_coll = db["payments"]
orders_coll = db["orders"]
orderdetails_coll = db["orderdetails"]
products_coll = db["products"]
productlines_coll = db["productlines"]

# CREATE DUMMY DATA FOR OFFICES COLL

# office_id
# city
# phone
# address_line_1
# address_line_2
# state
# country
# zip_code
# territory

# dummy_data = []
# for _ in range(10):
#     record = {
#         "office_id": fake.random_int(min=100, max=999),
#         "city": fake.city(),
#         "phone": fake.phone_number(),
#         "address_line_1": fake.street_address(),
#         "address_line_2": fake.secondary_address(),
#         "state": fake.state_abbr(),
#         "country": fake.country(),
#         "zip_code": fake.zipcode(),
#         "territory": fake.word()
#     }
#     dummy_data.append(record)

# insert_result = offices_coll.insert_many(dummy_data)

# # Print the inserted document IDs
# print("Inserted IDs:", insert_result.inserted_ids)

# CREATE DUMMY DATA FOR EMPLOYEES COLL 

# employee_id
# last_name
# first_name
# extension
# email
# office_id
# reports_to
# job_name

# doc = offices_coll.find({})

# offices_id = []

# for i in list(doc):
#     offices_id.append(i['office_id'])

# Sample data for job names
# job_names = ["Manager", "Sales Associate", "Marketing Specialist", "Accountant", "HR Coordinator"]

# Insert 35 fake records into the collection
# dummy_data = []
# for _ in range(35):
#     record = {
#         "employee_id": fake.random_number(digits=3),
#         "last_name": fake.last_name(),
#         "first_name": fake.first_name(),
#         "extension": fake.random_number(digits=4),
#         "email": fake.email(),
#         "office_id": random.choice(offices_id ),  # Assuming 10 different office IDs
#         "reports_to": random.choice([None] + [fake.uuid4() for _ in range(10)]),  # Randomly select a supervisor or None
#         "job_name": random.choice(job_names)
#     }
#     dummy_data.append(record)

# insert_result = employees_coll.insert_many(dummy_data)

# Print the inserted document IDs
# print("Inserted IDs:", insert_result.inserted_ids)

# CREATE DUMMY DATA FOR CLIENTS COLL

# client_id int
# client_name
# last_name
# first_name
# phone
# address_line_1
# address_line_2
# city
# state
# zip_code
# country
# sales_rep_employee_id 
# credit_limit float

# doc = employees_coll.find({})

# employees_id = []

# for i in list(doc):
#      employees_id.append(i['employee_id'])

# dummy_data = []
# for _ in range(50):
#     record = {
#         "client_id": random.randint(1000, 9999),  # Assuming client IDs are four-digit integers
#         "client_name": fake.company(),
#         "last_name": fake.last_name(),
#         "first_name": fake.first_name(),
#         "phone": fake.phone_number(),
#         "address_line_1": fake.street_address(),
#         "address_line_2": fake.secondary_address(),
#         "city": fake.city(),
#         "state": fake.state_abbr(),
#         "zip_code": fake.zipcode(),
#         "country": fake.country(),
#         "sales_rep_employee_id": random.choice(employees_id),
#         "credit_limit": round(random.uniform(1000.0, 10000.0), 2)  # Random credit limit between 1000 and 10000
#     }
#     dummy_data.append(record)

# insert_result = clients_coll.insert_many(dummy_data)

# Print the inserted document IDs
# print("Inserted IDs:", insert_result.inserted_ids)

# CREATE DUMMY DATA FOR PAYMENTS COLL 

# client_id
# check_id string
# payments_date date
# amount float

# doc = clients_coll.find({})

# clients_id = []

# for i in list(doc):
#     clients_id.append(i['client_id'])

# print(clients_id)

# dummy_data = []
# for _ in range(100):
#     payment_date = datetime.now() - timedelta(days=random.randint(1, 365))
#     record = {
#         "client_id": random.choice(clients_id),  # Assuming client IDs are four-digit integers
#         "check_id": fake.uuid4(),
#         "payment_date": payment_date,
#         "amount": round(random.uniform(100.0, 1000.0), 2)  # Random amount between 100 and 1000
#     }
#     dummy_data.append(record)

# insert_result = payments_coll.insert_many(dummy_data)

# Print the inserted document IDs
# print("Inserted IDs:", insert_result.inserted_ids)

# CREATE DUMMY DATA FOR ORDERS COLL

# order_id 
# order_date
# required_date 
# shipped_date
# status
# comments
# client_id 

# doc = clients_coll.find({})

# clients_id = []

# for i in list(doc):
#     clients_id.append(i['client_id'])

# order_statuses = ["Pending", "Shipped", "Delivered", "Cancelled"]

# dummy_data = []
# for i in range(150):
#     order_date = datetime.now() - timedelta(days=random.randint(1, 365))  # Random date within the last year
#     required_date = order_date + timedelta(days=random.randint(1, 30))  # Random required date within 30 days
#     shipped_date = order_date + timedelta(days=random.randint(1, 15))  # Random shipped date within 15 days of order
#     status = random.choice(order_statuses)
    
#     record = {
#         "order_id": i,
#         "order_date": order_date,
#         "required_date": required_date,
#         "shipped_date": shipped_date if status == "Shipped" else None,
#         "status": status,
#         "comments": fake.text(max_nb_chars=100),
#         "client_id": random.choice(clients_id)  # Assuming client IDs are four-digit integers
#     }
#     dummy_data.append(record)

# insert_result = orders_coll.insert_many(dummy_data)

# Print the inserted document IDs
# print("Inserted IDs:", insert_result.inserted_ids)

# CREATE DUMMY DATA FOR ORDERDETAILS COLL

# order_id int
# product_id 
# quantity int
# price_one float
# order_line int

# doc = orders_coll.find({})

# orders_id = []

# for i in list(doc):
#     orders_id.append(i['order_id'])

# doc2 = products_coll.find({})

# product_id = []

# for i in list(doc2):
#     product_id.append(i['product_id'])

# dummy_data = []
# for i in orders_id:
#     record = {
#         "order_id": i,  # Assuming order IDs are four-digit integers
#         "product_id": random.choice(product_id),
#         "quantity": random.randint(1, 10),
#         "price_one": round(random.uniform(10.0, 100.0), 2),  # Random price between 10 and 100
#         "order_line": fake.random_number(digits=4)
#     }
#     dummy_data.append(record)

# insert_result = orderdetails_coll.insert_many(dummy_data)

# Print the inserted document IDs
# print("Inserted IDs:", insert_result.inserted_ids)

# CREATE DUMMY DATA FOR PRODUCT COLL

# product_id string
# product_name string
# product_line string
# product_scale string
# supplier string
# description string
# stock int
# price float
# MSRP float

# doc = productlines_coll.find({})

# product_lines = []

# for i in list(doc):
#     product_lines.append(i['product_line'])

# product_scales = ["1:12", "1:18", "1:24", "1:32", "1:50", "1:72"]

# dummy_data = []
# for _ in range(25):
#     record = {
#         "product_id": fake.uuid4(),
#         "product_name": fake.company(),
#         "product_line": random.choice(product_lines),
#         "product_scale": random.choice(product_scales),
#         "supplier": fake.company(),
#         "description": fake.text(max_nb_chars=200),
#         "stock": fake.random_int(min=10, max=100),
#         "price": round(random.uniform(50.0, 500.0), 2),
#         "MSRP": round(random.uniform(100.0, 1000.0), 2)
#     }
#     dummy_data.append(record)

# insert_result = products_coll.insert_many(dummy_data)

# # Print the inserted document IDs
# print("Inserted IDs:", insert_result.inserted_ids)

# CREATE DUMMY DATA FOR PRODUCTLINES COLL

# product_line
# text_description

# dummy_data = []
# for _ in range(8):
#     record = {
#         "product_line": fake.word(),
#         "text_description": fake.text(max_nb_chars=200)
#     }
#     dummy_data.append(record)

# insert_result = productlines_coll.insert_many(dummy_data)

# Print the inserted document IDs
# print("Inserted IDs:", insert_result.inserted_ids)