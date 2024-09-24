# MongoDB with Python
This markdown file will show how to load the JSONs and how to query with aggregation in MongoDB with python.

## 1. Loading JSONs
### 1.1 Import necessary libraries
```python
from pymongo import MongoClient
import json
from tqdm import tqdm
```
### 1.2 Connect to MongoDB
```python
client = MongoClient ( 'localhost' , 27017)
```

### 1.3 Set a function to load the jsons
```python
def load_jsons_to_mongodb(client, filename):
    db = client['your_database_name']
    collection = db['your_collection_name']
    with open(filename, 'r') as f:
        f.readline()
        documents = []
        for line in tqdm(f, desc="Loading JSONs to MongoDB", unit="line"):
            if line.strip().endswith(','):
                line = line.strip()[:-1]
            if line.strip() == ']':
                break
            json_dict = json.loads(line)
            documents.append(json_dict)
            if len(documents) >= 500:  
                collection.insert_many(documents)
                documents = []
        if documents:
            collection.insert_many(documents)
```

### 1.4 Call the function
```python
load_jsons_to_mongodb ( client , 'all_lineorders.json')
```

## 2. Algorithm for Querying MongoDB with Python in Jupyter
### 2.1 Import necessary libraries
```python
from pymongo import MongoClient
```
### 2.2 Connect to MongoDB
```python
client = MongoClient('localhost', 27017)
```
### 2.3 Access the database
```python
db = client['your_database_name']
```

### 2.4 Access the collection
```python
collection = db['your_collection_name']
```
### 2.5 Query the collection
```python
query = {'key': 'value'}
results = collection.find(query)
```

### 2.6 Process the results
```python
for result in results:
    print(result)
```

## 3. Aggregation in MongoDB with python
### 3.1 Selects a specific field from a table where the key equals a value.
```python
collection.find({ ’key’: ’value’ }, { ’field’: 1})
```
### 3.2 Selects all fields from a table where the key equals a value.
```python
collection.find({ ’key’: ’value’ })
```
### 3.3 Inserts a new record into the table.
```python
collection.insert_one({ ’key’: key, ’field’:field, ’value’: value })
```

### 3.4 Updates a specific field in the table where the key equals a value.
```python
collection.update_one({ ’key’: ’value’ }, {’$set’: { ’field’: ’value’ } })
```

### 3.5 Deletes a record from the table where the key equals a value.
```python
collection.delete_one({ ’key’: ’value’ })
```
### 3.6 Selects specific fields from a table where the key equals a value.
```python
collection.find({ ’key’: ’value’ }, { ’field1’:1, ’field2’: 1 })
```
### 3.7 Checks if a record exists in the table where the key equals a value.
```python
collection.find({ ’key’: ’value’}).limit(1).count() > 0
```
### 3.8 Selects an aggregation of multiple columns to demonstrate how the operation is implemented in a NoSQL database.
```python
collection.aggregate([{ $match: {"LO_ORDERDATE.D_YEAR": "1993",
                                "LO_DISCOUNT":{$gte: 1, $lte: 3},
                                "LO_QUANTITY": {$gte:25} }},
                      {$group: {_id: null,
                                revenue:{$sum: {$multiply: ["$LO_EXTENDEDPRICE","$LO_DISCOUNT"]}}}}]);
```
