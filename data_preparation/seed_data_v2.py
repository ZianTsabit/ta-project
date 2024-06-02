import pymongo
import json

client = pymongo.MongoClient("mongodb://debezium:dbz@127.0.0.1:27017/db_shop?directConnection=true&serverSelectionTimeoutMS=2000&authSource=admin")
db = client["db_shop"]

collection_names = ["offices", "employees", "clients", "payments", "orders", "productlines", "products", "orderdetails"]

with open('mongodb_data.json', 'r') as f:
    
    data = json.load(f)

for i in collection_names:
    print(i)
    coll = db[i]
    recs = data[i]

    for i in recs:
        i.pop('_id', None)
        result = coll.insert_one(i)
        print(f"Inserted document ID: {result.inserted_id}")

print('finish')

