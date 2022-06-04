**YANPD - Yet Another Neo4j Python Driver**

YANPD is a Neo4j graph database driver that uses HTTP API to perform CRUD (Create, 
Read, Update, Delete) operations.

# Why another driver

Most of the drivers focus on individual graph database elements - nodes, links, 
attributes - YANPD on the other hand focuses on bulk interactions helping to 
populate Neo4j database using standard Python lists and dictionaries.

YNPD does not expect you to know how to interact with individual classes but rather
allows to load into and retrieve data from database using dictionary based requests. 

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

# TODO

Add **Data Validation** using YANG models.

<details><summary>Sample YANG model</summary>

module telecom-network {

    yang-version 1.1;

    namespace 'http://yanpd/network-device';

    prefix tn;

    revision 2022-01-10 {
        description "Initial revision";
    }

    container nodes {

        list network-device {
            key uuid;
            leaf uuid {
                type string;
                mandatory true;
            }
            leaf hostname {
                type string;
                mandatory true;
                description "Device hostname";
            }
            leaf-list labels {
                type string;
                min-elements 1;
				mandatory true;
                description "List of node labels";
            }
            leaf serial-number {
                type string;
            }
            leaf hardware-model {
                type string;
            }
            leaf status {
                type enumeration {
                    enum decomisionned;
                    enum production;
                    enum provisioning;
                }
            }
        }

        list vendor {
            key uuid;
            leaf uuid {
                type string;
                mandatory true;
            }
            leaf name {
                type string;
                mandatory true;
            }
        }

    }

    container links {

        list manufactured-by {
            leaf type {
                type string;
                must "current() = 'manufactured-by'" {
                    error-message "Link type must be 'manufactured-by'";
                }
				mandatory true;
            }
            leaf source {
                type leafref {
                    path "/nodes/network-device/uuid";
                }
                mandatory true;
            }
            leaf target {
                type leafref {
                    path "/nodes/vendor/uuid";
                }
                mandatory true;
            }
            leaf is-eof {
                type boolean;
                description "Is this platform EOF - end of life";
                default false;
            }
        }

    }
}
</details>

Add FastAPI based gateway to expose YANPD functionality using REST API.