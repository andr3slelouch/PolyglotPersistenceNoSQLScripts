# Redis with Python
This markdown file will show how to load the JSONs and how to query with aggregation in Redis with python.

## 1. Loading JSONs
### 1.1 Import necessary libraries
```python
import json
import time
from datetime import timedelta
from tqdm import tqdm
from redis import Redis
from redis.commands.json.path import Path
```
### 1.2 Connect to Redis
```python
client = Redis(host='localhost', port=6379, db=0)
```

### 1.3 Set a function to load the jsons
```python
def load_jsons_to_redis(client, filename):
    count = 0
    start_time = time.time()
    with open(filename, 'r') as f:
        f.readline()  # Skip the first line '['
        for line in tqdm(
            f, total=100, 
            desc="Loading JSONs to Redis", unit="line"):
            count += 1
            if line.strip().endswith(','):
                line = line.strip()[:-1]
            if line.strip() == ']':
                break
            json_dict = json.loads(line)  # Parse the JSON line
            client.json().set(
                f'lineorder:{count}', 
                Path.root_path(), 
                json_dict)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time taken: {str(timedelta(seconds=elapsed_time))}")
    print(f"Total JSON objects loaded: {count}")
```

### 1.4 Call the function
```python
load_jsons_to_redis(client, './path/to/file/all_lineorders.json')
```

## 2. Algorithm for Querying Redis with Python in Jupyter
### 2.1 Import necessary libraries
```python
import redis
```
### 2.2 Connect to Redis
```python
client = redis.Redis(host='localhost', port=6379, db=0)
```
### 2.3 Set a value (optional)
```python
client.set('key', 'value')
```

### 2.4 Get a value:
```python
value = client.get('key')
```
### 2.5 Process the result
```python
print(value.decode('utf-8'))
```

## 3. Aggregation in Redis with python
### 3.1 Selects a specific field from a table where the key equals a value.
```python
client.json_get(’lineitem:1743’,path=’.L_ORDERKEY’)
```
### 3.2 Selects all fields from a table where the key equals a value.
```python
client.json_get(’lineitem:1743’)
```
### 3.3 Inserts a new record into the table.
```python
client.json_set(’newkey’, ’.’, ’{"field":"value"}’)
```

### 3.4 Updates a specific field in the table where the key equals a value.
```python
client.json_set(’lineitem:1743’,path=’.L_ORDERKEY’, value=’"new value"’)
```

### 3.5 Deletes a record from the table where the key equals a value.
```python
client.delete(’lineitem:1743’)
```
### 3.6 Selects specific fields from a table where the key equals a value.
```python
client.json_mget(’lineitem:1743’,paths=[’.L_SHIPMODE’, ’.L_SHIPINSTRUCT’])
```
### 3.7 Checks if a record exists in the table where the key equals a value.
```python
client.exists(’lineitem:1743’)
```
### 3.8 Selects an aggregation of multiple columns to demonstrate how the operation is implemented in a NoSQL database.
### 3.8.1 Create an index
```python
client.ft().create index(’lineorder-idx’, ’ON
JSON PREFIX 1 "lineorder:" SCHEMA $.LO_ORDERKEY
AS LO_ORDERKEY NUMERIC $.LO_EXTENDEDPRICE
AS LO_EXTENDEDPRICE NUMERIC $.LO_DISCOUNT
AS LO_DISCOUNT NUMERIC $.LO_ORDERDATE.D_YEAR
AS D_YEAR TEXT $.LO_QUANTITY AS LO_QUANTITY
NUMERIC’)
```
### 3.8.2 Make the aggregation
```python
client.ft().aggregate(’lineorder-idx’, ’*’,
load=[’@LO_ORDERKEY’, ’@LO_EXTENDEDPRICE’,
’@LO_DISCOUNT’, ’@D_YEAR’, ’@LO_QUANTITY’],
filters=[’@D_YEAR == "1993"’, ’@LO_DISCOUNT
>= 1 && @LO_DISCOUNT <= 3’, ’@LO_QUANTITY <
25’], apply=[’@LO_EXTENDEDPRICE * @LO_DISCOUNT
AS revenue’], group by={’properties’:
[’@D YEAR’], ’reducers’: [(’SUM’, 1, ’revenue’,
’total revenue’)]})
```
