**YANPD - Yet Another Neo4j Python Driver**

YANPD is a Neo4j graph database driver that uses HTTP API to perform CRUD (Create, 
Read, Update, Delete) operations.

# Why another driver

Most of the drivers focus on individual graph database elements - nodes, links, 
attributes - YANPD on the other hand focuses on bulk interactions helping to 
populate Neo4j database using standard Python lists and dictionaries.

YNPD does not expect you to know how to interact with individual classes but rather
allows to load into and retrieve data from database using dictionary based requests. 

**Data Validation** 

# What it can do

Create, Read, Update, Delete operations for nodes, links and attributes.

# How it works

YANPD uses Neo4j HTTP API to ingrate with Neo4j database together with YANGSON for 
data validation using YANG models.

# Compatibility

- Neo4j 4.3.3 Community - tested

# Sample Usage

```python
from yanpd import neoconnector

# define your data
data = {
    "nodes": [
        {
            "labels": ["PERSON", "Engineer"],
            "uuid": 1234,
            "age": 40,
            "weight": 70,
            "name": "Gus",
        },
        {
            "labels": ["PERSON", "Teacher"],
            "uuid": 5678,
            "age": 35,
            "weight": 50,
            "name": "Eve",
        },
    ],
    "links": [
        {"type": "KNOWS", "source": 1234, "target": 5678, "since": "1995"},
    ],
}

# connect to Neo4j database
connection = {
    "url": "http://1.2.3.4:7474/",
    "username": "neo4j",
    "password": "neo4j",
    "database": "neo4j"
}
conn = neoconnector(**connection)

# push data to graph database
conn.from_dict(data)
```
