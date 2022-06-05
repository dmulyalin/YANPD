import sys
import pprint
import logging
import uuid

sys.path.insert(0, "..")

from yanpd import neoconnector, yang_validator
from config import conn_data

def test_yang_validator_dict_data():
    data = {
        "nodes": {
            "network-device": [
                {
                    "uuid": "device-1",
                    "hostname": "R1",
                    "labels": ["router", "access"],
                    "hardware-model": "ABC1099-X",
                },
                {
                    "uuid": "device-2",
                    "hostname": "R2",
                    "labels": ["router", "agg"],
                    "hardware-model": "ABC9999-X",
                },
            ],
            "vendor": [
                {
                    "uuid": "vendor-1",
                    "name": "Network Vendor XYZ"
                }
            ]
        },
        "links": {
            "manufactured-by": [
                {
                    "type": "manufactured-by",
                    "source": "device-1",
                    "target": "vendor-1",
                    "is-eof": False,
                },
                {
                    "type": "manufactured-by",
                    "source": "device-2",
                    "target": "vendor-1",
                    "is-eof": False,
                }
            ]
        }
    }
    validator = yang_validator(models_dir="./models/")
    # run validation, exception raised if validation failed
    assert validator.validate(data, model_name="telecom-network") == True, "Validation failed"

# test_yang_validator_dict_data()

def test_from_dict_with_model():    
    conn = neoconnector(**conn_data, models_dir="./models/")
    
    # clear all nodes/links
    conn.run("delete_all")
    
    data = {
        "nodes": {
            "network-device": [
                {
                    "uuid": "device-1",
                    "hostname": "R1",
                    "labels": ["router", "access"],
                    "hardware-model": "ABC1099-X",
                },
                {
                    "uuid": "device-2",
                    "hostname": "R2",
                    "labels": ["router", "agg"],
                    "hardware-model": "ABC9999-X",
                },
            ],
            "vendor": [
                {
                    "uuid": "vendor-1",
                    "name": "Network Vendor XYZ"
                }
            ]
        },
        "links": {
            "manufactured-by": [
                {
                    "source": "device-1",
                    "target": "vendor-1",
                    "is-eof": False,
                },
                {
                    "type": "manufactured-by",
                    "source": "device-2",
                    "target": "vendor-1",
                    "is-eof": False,
                }
            ]
        }
    }

    res = conn.from_dict(data, model_name="telecom-network")
    
    n1_details = conn.get_node(uuid="device-1")
    n2_details = conn.get_node(uuid="device-2")

    print("\nTransaction results: ")
    pprint.pprint(res)
    print("Device 1 details: ")
    pprint.pprint(n1_details)
    print("Device 2 details: ")
    pprint.pprint(n2_details)
    
    assert len(res) == 4, "Was expecting 4 items, 2 for nodes and 2 for links"
    assert n1_details["results"][0]["data"][0]["row"][0]["uuid"] == "device-1"
    assert n2_details["results"][0]["data"][0]["row"][0]["uuid"] == "device-2"
    