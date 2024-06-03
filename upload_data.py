import pandas as pd
import numpy as np
import json


from pymongo.mongo_client import MongoClient

uri = "mongodb+srv://subhamlai:Subham2002@cluster0.0f0ltjo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)



# Create a new client and connect to the server
client = MongoClient(uri)

# db create 
db = client["sensor_fault"]

# collection 
coll = db["sensor_data"]


# convert data to json file 
df = pd.read_csv("/config/workspace/notebooks/wafer_23012020_041211.csv")
# df.head()

df.drop("Unnamed: 0",axis=1,inplace=True)
# convert df into json
json_record = list(json.loads(df.T.to_json()).values())
# json_record

coll.insert_many(json_record)