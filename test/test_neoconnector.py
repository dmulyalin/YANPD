import sys
import pprint
import logging
import uuid

sys.path.insert(0, "..")

from yanpd import neoconnector

conn_data = {
    "url": "http://192.168.1.120:7474/",
    "username": "neo4j",
    "password": "s3cr3t",
}


def test_discovery_api():
    conn = neoconnector(**conn_data)
    # pprint.pprint(conn.settings)
    assert (
        "transaction" in conn.settings
    ), "Failed to communicate with Neo4j Discovery API"
    assert "in_one_go" in conn.settings, "Failed to form 'in one go' URI"


# test_discovery_api()


def test_create_node():
    conn = neoconnector(**conn_data)

    ret = conn.create_node(
        uuid=str(uuid.uuid1()),
        labels=["Person", "AU"],
        properties={"age": 40, "height": 180},
        dry_run=False,
    )
    assert ret == {"errors": [], "results": [{"columns": [], "data": []}]}


# test_create_node()


def test_run_delete_all():
    conn = neoconnector(**conn_data)
    ret = conn.run("delete_all")

    # pprint.pprint(ret)

    assert ret["errors"] == []
    assert "results" in ret
    assert len(ret["results"]) == 1
    assert "stats" in ret["results"][0]


# test_run_delete_all()


def test_create_node_and_uuid_constraint():
    conn = neoconnector(**conn_data)

    # delete all
    delelete = conn.run("delete_all")

    # create new node
    node1 = conn.create_node(
        uuid=str(uuid.uuid1()),
        labels=["Person", "AU"],
        properties={"age": 40, "height": 180},
        dry_run=False,
    )

    # add uuid unique constraint
    uniq_create = conn.run(
        "create_unique_constraint", {"property": "uuid"}, render=True
    )
    # pprint.pprint(uniq_create)
    assert uniq_create["errors"] == []
    assert "results" in uniq_create
    assert len(uniq_create["results"]) == 1


# test_create_node_and_uuid_constraint()


def test_create_link():
    conn = neoconnector(**conn_data)

    # create new nodes
    n1_uuid = str(uuid.uuid1())
    node1 = conn.create_node(
        uuid=n1_uuid,
        labels=["Person", "AU"],
        properties={"age": 40, "height": 180},
        dry_run=False,
    )
    n2_uuid = str(uuid.uuid1())
    node2 = conn.create_node(
        uuid=n2_uuid,
        labels=["Person", "AU"],
        properties={"age": 40, "height": 180},
        dry_run=False,
    )

    # create relationship
    link = conn.create_link(
        source=n1_uuid,
        target=n2_uuid,
        type="knows",
        properties={"age": 10},
        dry_run=False,
    )

    # pprint.pprint(link)
    #
    # {'errors': [],
    #  'results': [{'columns': ['r'],
    #               'data': [{'meta': [{'deleted': False,
    #                                    'id': 2,
    #                                    'type': 'relationship'}],
    #                            'row': [{'age': 10}]}]}]}
    assert link["errors"] == []
    assert "results" in link
    assert len(link["results"]) == 1


# test_create_link()


def test_tx_start():
    conn = neoconnector(**conn_data)

    tx, response = conn.tx_start()

    assert isinstance(tx, int)
    assert response["errors"] == []


# test_txt_start()


def test_tx_start_and_rollback():
    conn = neoconnector(**conn_data)

    tx, start = conn.tx_start()

    delete = conn.tx_rollback(tx)

    # pprint.pprint(delete)

    assert delete == True


# test_tx_start_and_rollback()


def test_tx_start_add_and_commit():
    conn = neoconnector(**conn_data)

    tx, start = conn.tx_start()

    add = conn.tx_add(
        statements=[
            {
                "statement": "CREATE (:DEFAULT:PERSON $props)",
                "parameters": {"props": {"uuid": str(uuid.uuid1()), "name": "Gus"}},
            }
        ],
        tx=tx,
    )
    # pprint.pprint(add)

    commit = conn.tx_commit(tx)

    # pprint.pprint(commit)
    assert commit["errors"] == []
    assert commit["results"] == []


# test_tx_start_add_and_commit()


def test_from_dict_method_create():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)
    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }

    res = conn.from_dict(data)
    n1_details = conn.get_node(uuid=n1)
    n2_details = conn.get_node(uuid=n2)

    # pprint.pprint(n1_details)
    # pprint.pprint(n2_details)

    assert len(res) == 4, "Not all transaction executed, should be 2 adds + 2 commits"
    assert len(n1_details["results"][0]["data"][0]["row"]) == 1
    assert len(n2_details["results"][0]["data"][0]["row"]) == 1


# test_from_dict_method_create()


def test_from_dict_method_merge():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)
    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }

    res = conn.from_dict(data, method="merge")
    n1_details = conn.get_node(uuid=n1)
    n2_details = conn.get_node(uuid=n2)

    # pprint.pprint(n1_details)
    # pprint.pprint(n2_details)

    assert len(res) == 4, "Not all transaction executed, should be 2 adds + 2 commits"
    assert len(n1_details["results"][0]["data"][0]["row"]) == 1
    assert len(n2_details["results"][0]["data"][0]["row"]) == 1


# test_from_dict_method_merge()


def test_get_node_all():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }
    # push data to graph
    conn.from_dict(data)

    # get all nodes
    res = conn.get_node()
    # pprint.pprint(res)
    # {'errors': [],
    #  'results': [{'columns': ['n'],
    #               'data': [{'meta': [{'deleted': False, 'id': 7, 'type': 'node'}],
    #                         'row': [{'age': 35,
    #                                  'name': 'Eve',
    #                                  'uuid': 'e6a98c09-f346-11eb-8eb2-0c54158bada2',
    #                                  'weight': 50}]},
    #                        {'meta': [{'deleted': False, 'id': 12, 'type': 'node'}],
    #                         'row': [{'age': 40,
    #                                  'name': 'Gus',
    #                                  'uuid': 'e6a98c08-f346-11eb-adbf-0c54158bada2',
    #                                  'weight': 70}]}]}]}

    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 2


# test_get_node_all()


def test_get_node_all_with_limit():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }
    # push data to graph
    conn.from_dict(data)

    # get all nodes
    res = conn.get_node(limit=1)
    # pprint.pprint(res)
    # {'errors': [],
    #  'results': [{'columns': ['n'],
    #               'data': [{'meta': [{'deleted': False, 'id': 7, 'type': 'node'}],
    #                         'row': [{'age': 35,
    #                                  'name': 'Eve',
    #                                  'uuid': 'e6a98c09-f346-11eb-8eb2-0c54158bada2',
    #                                  'weight': 50}]}]}]}

    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1


# test_get_node_all_with_limit()


def test_get_node_all_order_by_age():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }
    # push data to graph
    conn.from_dict(data)

    # get all nodes
    res = conn.get_node(order_by="age")
    # pprint.pprint(res)

    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 2
    assert res["results"][0]["data"][0]["row"][0]["age"] == 35
    assert res["results"][0]["data"][1]["row"][0]["age"] == 40


# test_get_node_all_order_by_age()


def test_get_node_all_order_by_age_descending():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }
    # push data to graph
    conn.from_dict(data)

    # get all nodes
    res_descending = conn.get_node(order_by="age", descending=True)
    # pprint.pprint(res_descending)

    assert res_descending["errors"] == []
    assert len(res_descending) == 2
    assert res_descending["results"][0]["data"][0]["row"][0]["age"] == 40
    assert res_descending["results"][0]["data"][1]["row"][0]["age"] == 35


# test_get_node_all_order_by_age_descending()


def test_get_node_all_with_skip():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }
    # push data to graph
    conn.from_dict(data)

    # get all nodes
    res = conn.get_node(skip=1)
    # pprint.pprint(res)

    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1


# test_get_node_all_with_skip()


def test_get_node_by_uuid():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }
    # push data to graph
    conn.from_dict(data)

    # get all nodes
    res = conn.get_node(uuid=n1)
    # pprint.pprint(res)

    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1
    assert res["results"][0]["data"][0]["row"][0]["name"] == "Gus"


# test_get_node_by_uuid()


def test_get_node_by_properties():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }
    # push data to graph
    conn.from_dict(data)

    # get all nodes
    res = conn.get_node(properties={"name": "Gus", "age": 40})
    # pprint.pprint(res)

    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1
    assert res["results"][0]["data"][0]["row"][0]["name"] == "Gus"


# test_get_node_by_properties()


def test_get_node_by_properties_with_labels():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
        ],
    }
    # push data to graph
    conn.from_dict(data)

    # get all nodes
    res = conn.get_node(properties={"name": "Gus", "age": 40}, labels=["PERSON"])
    # pprint.pprint(res)

    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1
    assert res["results"][0]["data"][0]["row"][0]["name"] == "Gus"


# test_get_node_by_properties_with_labels()


def test_update_node():
    n1 = str(uuid.uuid1())

    conn = neoconnector(**conn_data)
    node1 = conn.create_node(
        uuid=n1,
        labels=["Person", "AU"],
        properties={"age": 40, "height": 180, "name": "Mike", "engineer": True},
        dry_run=False,
    )
    before = conn.get_node(uuid=n1)

    # update node labels and properties
    res = conn.update_node(
        uuid=n1,
        properties={"engineer": False, "age": 50, "likes": "Coffe"},
        labels=["NSW", "Sydney"],
        dry_run=False,
    )
    after = conn.get_node(uuid=n1)
    # pprint.pprint(res)
    # pprint.pprint(before)
    # pprint.pprint(after)

    assert res["errors"] == []
    assert before != after
    assert after["results"][0]["data"][0]["row"][0]["age"] == 50
    assert after["results"][0]["data"][0]["row"][0]["engineer"] == False


# test_update_node()


def test_from_dict_performance():
    import random
    import time

    count = 100

    # uncomment below code to run testing

    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON"],
                "uuid": str(uuid.uuid1()),
                "age": random.randint(1, 100),
                "weight": random.randint(1, 100),
                "name": "Name {}".format(index),
            }
            for index, i in enumerate(range(count))
        ]
    }
    data["links"] = [
        {
            "type": "KNOWS",
            "source": data["nodes"][random.randint(0, count - 1)]["uuid"],
            "target": data["nodes"][random.randint(0, count - 1)]["uuid"],
            "how_long": random.randint(1, 100),
        }
        for i in range(count)
    ]

    # pprint.pprint(data)

    start = time.time()
    res = conn.from_dict(data, rate=10000, method="create")
    finish = time.time()
    elapsed = finish - start
    print(
        "Created {count} nodes and {count} links in {elapsed} seconds".format(
            count=count, elapsed=elapsed
        )
    )

    assert (
        elapsed < 2
    ), "{}s - too long to create {} links and {} nodes, should be below 2s".format(
        elapsed, count, count
    )


# test_from_dict_performance()


def test_delete_node_by_uuid():
    n1 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
        ]
    }
    # push data to graph
    conn.from_dict(data)

    node_details = conn.get_node(uuid=n1)
    delete_node = conn.delete_node(uuid=n1)
    node_details_after_delete = conn.get_node(uuid=n1)

    # pprint.pprint(node_details)
    # pprint.pprint(delete_node)
    # pprint.pprint(node_details_after_delete)

    assert node_details["errors"] == []
    assert len(node_details["results"][0]["data"][0]["row"]) == 1
    assert node_details["results"][0]["data"][0]["row"][0]["uuid"] == n1
    assert len(node_details_after_delete["results"][0]["data"]) == 0
    assert delete_node["errors"] == []


# test_delete_node_by_uuid()


def test_delete_node_by_labels_and_properties():
    n1 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
        ]
    }
    # push data to graph
    conn.from_dict(data)

    node_details = conn.get_node(uuid=n1)
    delete_node = conn.delete_node(
        labels=["Engineer"], properties={"age": 40, "weight": 70}
    )
    node_details_after_delete = conn.get_node(uuid=n1)

    # pprint.pprint(node_details)
    # pprint.pprint(delete_node)
    # pprint.pprint(node_details_after_delete)

    assert node_details["errors"] == []
    assert len(node_details["results"][0]["data"][0]["row"]) == 1
    assert node_details["results"][0]["data"][0]["row"][0]["uuid"] == n1
    assert len(node_details_after_delete["results"][0]["data"]) == 0
    assert delete_node["errors"] == []


# test_delete_node_by_labels_and_properties()

def test_get_link_all():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC"}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False)    
    # pprint.pprint(res)
    # {'errors': [],
    #  'results': [{'columns': ['link'],
    #               'data': [{'meta': [{'deleted': False,
    #                                   'id': 8738,
    #                                   'type': 'relationship'}],
    #                         'row': [{'employer': 'ARPNAC'}]},
    #                        {'meta': [{'deleted': False,
    #                                   'id': 8736,
    #                                   'type': 'relationship'}],
    #                         'row': [{'since': '1995'}]},
    #                        {'meta': [{'deleted': False,
    #                                   'id': 8737,
    #                                   'type': 'relationship'}],
    #                         'row': [{'friends': True}]}]}]}   
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 3
    
# test_get_link_all()


def test_get_link_skip():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC"}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, skip=1)    
    # pprint.pprint(res)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 2
    
# test_get_link_skip()


def test_get_link_limit():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC"}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, limit=2)    
    # pprint.pprint(res)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 2
    
# test_get_link_limit()

def test_get_link_by_type():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995"},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC"}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, type="WORK_WITH")    
    # pprint.pprint(res, width=100)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1
    
# test_get_link_by_type()


def test_get_link_by_property():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC"}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, properties={"since": 1995})    
    # pprint.pprint(res, width=100)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1
    
# test_get_link_by_property()


def test_get_link_by_property_and_type():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, properties={"employer": "ARPNAC", "rank": 10}, type="WORK_WITH")    
    # pprint.pprint(res, width=100)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1
    assert res["results"][0]["data"][0]["row"][0]["rank"] == 10
    
# test_get_link_by_property_and_type()


def test_get_link_by_source_labels():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, source_labels=["PERSON", "Doctor"])    
    # pprint.pprint(res, width=100)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 2
    assert res["results"][0]["data"][1]["row"][0]["rank"] == 10
    
# test_get_link_by_source_labels()

def test_get_link_by_source_labels_and_properties():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, source_labels=["PERSON", "Doctor"], source_properties={"age": 29})    
    # pprint.pprint(res, width=100)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 2
    assert res["results"][0]["data"][1]["row"][0]["rank"] == 10
   
# test_get_link_by_source_labels_and_properties()


def test_get_link_by_target_labels():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, target_labels=["PERSON", "Engineer"])    
    # pprint.pprint(res, width=100)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 2
    assert res["results"][0]["data"][0]["row"][0]["since"] == 1995
    
# test_get_link_by_target_labels()


def test_get_link_by_target_labels_and_properties():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn_new = neoconnector(**conn_data)

    # clear all nodes/links
    conn_new.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    conn_new.from_dict(data)
    res_dry = conn_new.get_link(dry_run=True, target_labels=["PERSON", "Engineer"], target_properties={"age": 40})
    # pprint.pprint(res_dry)
    # get all links                                        
    res = conn_new.get_link(dry_run=False, target_labels=["PERSON", "Engineer"], target_properties={"age": 40})   
    # pprint.pprint(res, width=100)
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 2, res_dry
    
# test_get_link_by_target_labels_and_properties()


def test_get_link_by_source_uuid():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, source=n1)    
    # pprint.pprint(res, width=100)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 2

# test_get_link_by_source_uuid()


def test_get_link_by_source_and_target_uuid():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, source=n1, target=n2)    
    # pprint.pprint(res, width=100)
  
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1
    assert res["results"][0]["data"][0]["row"][0]["since"] == 1995
    
# test_get_link_by_source_and_target_uuid()

def get_link_all_order_by_and_desc():
    """
    Test two queries one with another without descending order
    """
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": "1995", "order": 1},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True, "order": 2},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "order": 3}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # get all links
    res = conn.get_link(dry_run=False, order_by="order")   
    res_desc = conn.get_link(dry_run=False, order_by="order", descending=True)           
    # pprint.pprint(res)
    # pprint.pprint(res_desc)
    assert res['errors'] == []
    assert res["results"][0]["data"][0]["row"][0]["order"] == 1
    assert res["results"][0]["data"][1]["row"][0]["order"] == 2
    assert res["results"][0]["data"][2]["row"][0]["order"] == 3                                           
    assert res_desc['errors'] == []
    assert res_desc["results"][0]["data"][0]["row"][0]["order"] == 3
    assert res_desc["results"][0]["data"][1]["row"][0]["order"] == 2
    assert res_desc["results"][0]["data"][2]["row"][0]["order"] == 1   

# get_link_all_order_by_and_desc()
    
def test_get_link_by_type_and_properties_source_and_target_labels_and_properties():
    """
    Test them all
    """
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn_new = neoconnector(**conn_data)

    # clear all nodes/links
    conn_new.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    conn_new.from_dict(data)
    res_dry = conn_new.get_link(
        dry_run=True, 
        target_labels=["PERSON", "Engineer"], 
        target_properties={"age": 40},
        source_labels=["PERSON"],
        source_properties={"age": 29},
        type="WORK_WITH",
        properties={"rank": 10, "employer": "ARPNAC"}
    )
    should_contain = ["PERSON", "Engineer", "age", "WORK_WITH", "rank", "employer", "ARPNAC"]
    assert all([i in res_dry["statement"] for i in should_contain]), "query malformed and does not contains all parameters: {}".format(res_dry)
    # pprint.pprint(res_dry, width=150)
    # get all links                                        
    res = conn_new.get_link(
        dry_run=False, 
        target_labels=["PERSON", "Engineer"], 
        target_properties={"age": 40},
        source_labels=["PERSON"],
        source_properties={"age": 29},
        type="WORK_WITH",
        properties={"rank": 10, "employer": "ARPNAC"}
    )   
    # pprint.pprint(res, width=100)
    assert res["errors"] == []
    assert len(res["results"][0]["data"]) == 1
    assert res["results"][0]["data"][0]["row"][0]["rank"] == 10
    
# test_get_link_by_type_and_properties_source_and_target_labels_and_properties()

def test_delete_link():
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn = neoconnector(**conn_data)

    # clear all nodes/links
    conn.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    conn.from_dict(data)

    # update link
    res = conn.delete_link(dry_run=False, source=n3, target=n1, type="WORK_WITH", properties={"rank": 10})    
    # pprint.pprint(res, width=100)
    assert res["errors"] == []
    
    # try to get deleted link
    get_del_res = conn.get_link(dry_run=False, properties={"rank": 10})
    # pprint.pprint(get_del_res)

    assert get_del_res["errors"] == []
    assert len(get_del_res["results"][0]["data"]) == 0
    
# test_delete_link()

def test_link_update_properties():
    """
    This test fails if run with other tests but succeed if run individually
    """
    n1 = str(uuid.uuid1())
    n2 = str(uuid.uuid1())
    n3 = str(uuid.uuid1())
    conn1 = neoconnector(**conn_data)

    # clear all nodes/links
    conn1.run("delete_all")

    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": n1,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": n2,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
            {
                "labels": ["PERSON", "Doctor"],
                "uuid": n3,
                "age": 29,
                "weight": 75,
                "name": "Steve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": n1, "target": n2, "since": 1995},
            {"type": "KNOWS", "source": n2, "target": n3, "friends": True},
            {"type": "WORK_WITH", "source": n3, "target": n1, "employer": "ARPNAC", "rank": 10}
        ],
    }    
    
    # push data to graph
    create = conn1.from_dict(data)
    # pprint.pprint(create)
    
    # update link
    res = conn1.update_link(dry_run=False, source=n1, target=n2, type="KNOWS", new_properties={"grade": 1, "cost": 20})    
    # pprint.pprint(res, width=100)
  
    # get updated link
    get_res = conn1.get_link(dry_run=False, properties={"cost": 20})
    # pprint.pprint(get_res)

    assert get_res["errors"] == []
    assert len(get_res["results"][0]["data"]) == 1, "Try run this test individually, if fails, we have a problem- pytest test_neoconnector.py::test_link_update_properties"
    assert get_res["results"][0]["data"][0]["row"][0]["cost"] == 20
    
# test_link_update_properties()