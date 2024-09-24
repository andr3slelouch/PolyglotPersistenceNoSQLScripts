# Cassandra with Python
This markdown file will show how to load the JSONs and how to query with aggregation in Cassandra with python.

## 1. Loading JSONs
### 1.1 Import necessary libraries
```python
from cassandra.cluster import Cluster
import json
from tqdm import tqdm
```
### 1.2 Connect to Cassandra
```python
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('your_keyspace')
```
### 1.3 Create the keyspace (database)
```python
keyspace_creation_query = "CREATE KEYSPACE IF NOT EXISTS json WITH replication = 
{'class': 'SimpleStrategy', 'replication_factor': 1};"
session.execute(keyspace_creation_query)
session.execute("USE json;")
```
### 1.4 Creation of the tables
#### 1.4.1 Creation of the table json.customer
```python
customer_type_creation_query = """
CREATE TYPE json.customer (
    C_CUSTKEY int,
    C_NAME text,
    C_ADDRESS text,
    C_CITY text,
    C_NATION text,
    C_REGION text,
    C_PHONE text,
    C_MKTSEGMENT text
);
"""
session.execute(customer_type_creation_query)
```
#### 1.4.2 Creation of the table json.part
```python
cpart_type_creation_query = """
CREATE TYPE json.part (
    P_PARTKEY int,
    P_NAME text,
    P_MFGR text,
    P_CATEGORY text,
    P_BRAND1 text,
    P_COLOR text,
    P_TYPE text,
    P_SIZE int,
    P_CONTAINER text
);
"""
session.execute(part_type_creation_query)
```

#### 1.4.3 Creation of the table json.supplier
```python
supplier_type_creation_query = """
CREATE TYPE json.supplier (
    S_SUPPKEY int,
    S_NAME text,
    S_ADDRESS text,
    S_CITY text,
    S_NATION text,
    S_REGION text,
    S_PHONE text
);
"""
session.execute(supplier_type_creation_query)

```

#### 1.4.4 Creation of the table json.date
```python
date_type_creation_query = """
CREATE TYPE json."date" (
    D_DATEKEY int,
    D_DATE text,
    D_DAYOFWEEK text,
    D_MONTH text,
    D_YEAR text,
    D_YEARMONTHNUM int,
    D_YEARMONTH text,
    D_DAYNUMINWEEK int,
    D_DAYNUMINMONTH int,
    D_DAYNUMINYEAR int,
    D_MONTHNUMINYEAR int,
    D_WEEKNUMINYEAR int,
    D_SELLINGSEASON text,
    D_LASTDAYINWEEKFL boolean,
    D_LASTDAYINMONTHFL boolean,
    D_HOLIDAYFL boolean,
    D_WEEKDAYFL boolean
);
"""
session.execute(date_type_creation_query)

```
#### 1.4.5 Creation of the table json.lineorders
```python
table_creation_query = """
CREATE TABLE json.lineorders (
    LO_ORDERKEY int PRIMARY KEY,
    LO_LINENUMBER int,
    CUSTOMER frozen<customer>,
    PART frozen<part>,
    SUPPLIER frozen<supplier>,
    LO_ORDERDATE frozen<"date">,
    LO_COMMITDATE frozen<"date">,
    LO_ORDERPRIORITY text,
    LO_SHIPPRIORITY text,
    LO_QUANTITY int,
    LO_EXTENDEDPRICE int,
    LO_ORDTOTALPRICE int,
    LO_DISCOUNT int,
    LO_REVENUE int,
    LO_SUPPLYCOST int,
    LO_TAX int,
    LO_SHIPMODE text
);
"""
session.execute(table_creation_query)

```

### 1.5 Set a function to load the jsons
```python
def insert_data(session, filename):
    with open(filename, 'r') as f:
        f.readline()  # Skip the '[' at the beginning
        for line in tqdm(f, desc="Inserting data", unit="line"):
            if line.strip().endswith(','):
                line = line.strip()[:-1]
            if line.strip() == ']':
                break

            # Parse the JSON line into a Python dictionary
            data = json.loads(line)
            
            # Convert nested fields to match Cassandra's UDT types
            customer = convert_customer(data['CUSTOMER'])
            part = convert_part(data['PART'])
            supplier = convert_supplier(data['SUPPLIER'])
            order_date = convert_date(data['LO_ORDERDATE'])
            commit_date = convert_date(data['LO_COMMITDATE'])

            # Prepare the INSERT statement using manually mapped data
            query = """
            INSERT INTO json.lineorders (
                LO_ORDERKEY, LO_LINENUMBER, CUSTOMER, PART, SUPPLIER, LO_ORDERDATE, LO_COMMITDATE,
                LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_ORDTOTALPRICE,
                LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, LO_TAX, LO_SHIPMODE
            ) VALUES (
                {LO_ORDERKEY}, {LO_LINENUMBER},
                {{C_CUSTKEY: {C_CUSTKEY}, C_NAME: '{C_NAME}', C_ADDRESS: '{C_ADDRESS}', C_CITY: '{C_CITY}', 
                C_NATION: '{C_NATION}', C_REGION: '{C_REGION}', C_PHONE: '{C_PHONE}', C_MKTSEGMENT: '{C_MKTSEGMENT}'}},
                {{P_PARTKEY: {P_PARTKEY}, P_NAME: '{P_NAME}', P_MFGR: '{P_MFGR}', P_CATEGORY: '{P_CATEGORY}', 
                P_BRAND1: '{P_BRAND1}', P_COLOR: '{P_COLOR}', P_TYPE: '{P_TYPE}', P_SIZE: {P_SIZE}, 
                P_CONTAINER: '{P_CONTAINER}'}},
                {{S_SUPPKEY: {S_SUPPKEY}, S_NAME: '{S_NAME}', S_ADDRESS: '{S_ADDRESS}', S_CITY: '{S_CITY}', 
                S_NATION: '{S_NATION}', S_REGION: '{S_REGION}', S_PHONE: '{S_PHONE}'}},
                {{D_DATEKEY: {D_DATEKEY}, D_DATE: '{D_DATE}', D_DAYOFWEEK: '{D_DAYOFWEEK}', D_MONTH: '{D_MONTH}', 
                D_YEAR: '{D_YEAR}', D_YEARMONTHNUM: {D_YEARMONTHNUM}, D_YEARMONTH: '{D_YEARMONTH}', 
                D_DAYNUMINWEEK: {D_DAYNUMINWEEK}, D_DAYNUMINMONTH: {D_DAYNUMINMONTH}, D_DAYNUMINYEAR: {D_DAYNUMINYEAR}, 
                D_MONTHNUMINYEAR: {D_MONTHNUMINYEAR}, D_WEEKNUMINYEAR: {D_WEEKNUMINYEAR}, 
                D_SELLINGSEASON: '{D_SELLINGSEASON}', D_LASTDAYINWEEKFL: {D_LASTDAYINWEEKFL}, 
                D_LASTDAYINMONTHFL: {D_LASTDAYINMONTHFL}, D_HOLIDAYFL: {D_HOLIDAYFL}, D_WEEKDAYFL: {D_WEEKDAYFL}}},
                {{D_DATEKEY: {D_DATEKEY_COMMIT}, D_DATE: '{D_DATE_COMMIT}', D_DAYOFWEEK: '{D_DAYOFWEEK_COMMIT}', 
                D_MONTH: '{D_MONTH_COMMIT}', D_YEAR: '{D_YEAR_COMMIT}', D_YEARMONTHNUM: {D_YEARMONTHNUM_COMMIT}, 
                D_YEARMONTH: '{D_YEARMONTH_COMMIT}', D_DAYNUMINWEEK: {D_DAYNUMINWEEK_COMMIT}, 
                D_DAYNUMINMONTH: {D_DAYNUMINMONTH_COMMIT}, D_DAYNUMINYEAR: {D_DAYNUMINYEAR_COMMIT}, 
                D_MONTHNUMINYEAR: {D_MONTHNUMINYEAR_COMMIT}, D_WEEKNUMINYEAR: {D_WEEKNUMINYEAR_COMMIT}, 
                D_SELLINGSEASON: '{D_SELLINGSEASON_COMMIT}', D_LASTDAYINWEEKFL: {D_LASTDAYINWEEKFL_COMMIT}, 
                D_LASTDAYINMONTHFL: {D_LASTDAYINMONTHFL_COMMIT}, D_HOLIDAYFL: {D_HOLIDAYFL_COMMIT}, 
                D_WEEKDAYFL: {D_WEEKDAYFL_COMMIT}}},
                '{LO_ORDERPRIORITY}', '{LO_SHIPPRIORITY}', {LO_QUANTITY}, {LO_EXTENDEDPRICE}, {LO_ORDTOTALPRICE},
                {LO_DISCOUNT}, {LO_REVENUE}, {LO_SUPPLYCOST}, {LO_TAX}, '{LO_SHIPMODE}'
            );
            """.format(
                LO_ORDERKEY=data['LO_ORDERKEY'], LO_LINENUMBER=data['LO_LINENUMBER'],
                C_CUSTKEY=customer['C_CUSTKEY'], C_NAME=customer['C_NAME'], C_ADDRESS=customer['C_ADDRESS'], C_CITY=customer['C_CITY'], 
                C_NATION=customer['C_NATION'], C_REGION=customer['C_REGION'], C_PHONE=customer['C_PHONE'], C_MKTSEGMENT=customer['C_MKTSEGMENT'],
                P_PARTKEY=part['P_PARTKEY'], P_NAME=part['P_NAME'], P_MFGR=part['P_MFGR'], 
                P_CATEGORY=part['P_CATEGORY'], P_BRAND1=part['P_BRAND1'], P_COLOR=part['P_COLOR'], 
                P_TYPE=part['P_TYPE'], P_SIZE=part['P_SIZE'], P_CONTAINER=part['P_CONTAINER'],
                
                S_SUPPKEY=supplier['S_SUPPKEY'], S_NAME=supplier['S_NAME'], S_ADDRESS=supplier['S_ADDRESS'], 
                S_CITY=supplier['S_CITY'], S_NATION=supplier['S_NATION'], S_REGION=supplier['S_REGION'], 
                S_PHONE=supplier['S_PHONE'],
                
                D_DATEKEY=order_date['D_DATEKEY'], D_DATE=order_date['D_DATE'], 
                D_DAYOFWEEK=order_date['D_DAYOFWEEK'], D_MONTH=order_date['D_MONTH'], 
                D_YEAR=order_date['D_YEAR'], D_YEARMONTHNUM=order_date['D_YEARMONTHNUM'], 
                D_YEARMONTH=order_date['D_YEARMONTH'], D_DAYNUMINWEEK=order_date['D_DAYNUMINWEEK'], 
                D_DAYNUMINMONTH=order_date['D_DAYNUMINMONTH'], D_DAYNUMINYEAR=order_date['D_DAYNUMINYEAR'], 
                D_MONTHNUMINYEAR=order_date['D_MONTHNUMINYEAR'], D_WEEKNUMINYEAR=order_date['D_WEEKNUMINYEAR'], 
                D_SELLINGSEASON=order_date['D_SELLINGSEASON'], D_LASTDAYINWEEKFL=order_date['D_LASTDAYINWEEKFL'], 
                D_LASTDAYINMONTHFL=order_date['D_LASTDAYINMONTHFL'], D_HOLIDAYFL=order_date['D_HOLIDAYFL'], 
                D_WEEKDAYFL=order_date['D_WEEKDAYFL'],
                
                D_DATEKEY_COMMIT=commit_date['D_DATEKEY'], D_DATE_COMMIT=commit_date['D_DATE'], 
                D_DAYOFWEEK_COMMIT=commit_date['D_DAYOFWEEK'], D_MONTH_COMMIT=commit_date['D_MONTH'], 
                D_YEAR_COMMIT=commit_date['D_YEAR'], D_YEARMONTHNUM_COMMIT=commit_date['D_YEARMONTHNUM'], 
                D_YEARMONTH_COMMIT=commit_date['D_YEARMONTH'], D_DAYNUMINWEEK_COMMIT=commit_date['D_DAYNUMINWEEK'], 
                D_DAYNUMINMONTH_COMMIT=commit_date['D_DAYNUMINMONTH'], D_DAYNUMINYEAR_COMMIT=commit_date['D_DAYNUMINYEAR'], 
                D_MONTHNUMINYEAR_COMMIT=commit_date['D_MONTHNUMINYEAR'], D_WEEKNUMINYEAR_COMMIT=commit_date['D_WEEKNUMINYEAR'], 
                D_SELLINGSEASON_COMMIT=commit_date['D_SELLINGSEASON'], D_LASTDAYINWEEKFL_COMMIT=commit_date['D_LASTDAYINWEEKFL'], 
                D_LASTDAYINMONTHFL_COMMIT=commit_date['D_LASTDAYINMONTHFL'], D_HOLIDAYFL_COMMIT=commit_date['D_HOLIDAYFL'], 
                D_WEEKDAYFL_COMMIT=commit_date['D_WEEKDAYFL'],
                
                LO_ORDERPRIORITY=data['LO_ORDERPRIORITY'], LO_SHIPPRIORITY=data['LO_SHIPPRIORITY'],
                LO_QUANTITY=data['LO_QUANTITY'], LO_EXTENDEDPRICE=int(data['LO_EXTENDEDPRICE']), 
                LO_ORDTOTALPRICE=int(data['LO_ORDTOTALPRICE']), LO_DISCOUNT=int(data['LO_DISCOUNT']), 
                LO_REVENUE=int(data['LO_REVENUE']), LO_SUPPLYCOST=int(data['LO_SUPPLYCOST']), 
                LO_TAX=int(data['LO_TAX']), LO_SHIPMODE=data['LO_SHIPMODE']
            )
            # Print the formatted query for debugging
            #print(query)
            
            # Execute the query
            session.execute(query)
```

### 1.4 Call the function
```python
insert_data(session, 'all_lineorders.json')
```

## 2. Algorithm for Querying Cassandra with Python in Jupyter
### 2.1 Import necessary libraries
```python
from cassandra.cluster import Cluster
```
### 2.2 Connect to Cassandra
```python
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('your_keyspace')
```
### 2.3 Access the keyspace (database)
```python
session.set_keyspace('your_keyspace')
```

### 2.4 Define and execute a query:
```python
query = "SELECT * FROM your_table WHERE key = 'value'"
rows = session.execute(query)
```
### 2.5 Process the result
```python
for row in rows:
    print(row)
```

## 3. Aggregation in Cassandra with python
### 3.1 Selects a specific field from a table where the key equals a value.
```python
session.execute("SELECT field FROM table WHERE key = ’value’ ALLOW FILTERING")
```
### 3.2 Selects all fields from a table where the key equals a value.
```python
session.execute("SELECT * FROM table WHERE key =’value’ ALLOW FILTERING")
```
### 3.3 Inserts a new record into the table.
```python
session.execute("INSERT INTO table (key, field,value) VALUES (’key’, ’field’, ’value’))
```

### 3.4 Updates a specific field in the table where the key equals a value.
```python
session.execute("UPDATE table SET field = ’value’ WHERE key = ’value’")
```

### 3.5 Deletes a record from the table where the key equals a value.
```python
session.execute("DELETE FROM table WHERE key = ’value’")
```
### 3.6 Selects specific fields from a table where the key equals a value.
```python
session.execute("SELECT field1, field2 FROM table WHERE key = ’value’ ALLOW FILTERING")
```
### 3.7 Checks if a record exists in the table where the key equals a value.
```python
session.execute("SELECT COUNT(*) FROM table WHERE key = ’value’ ALLOW FILTERING").one()[0] > 0
```
### 3.8 Selects an aggregation of multiple columns to demonstrate how the operation is implemented in a NoSQL database.
```python
session.execute("SELECT lo_extendedprice,
lo_discount FROM json.lineorders WHERE
lo_discount >= 1 AND lo_discount <= 3 AND
lo_quantity < 25 ALLOW FILTERING;")
```
