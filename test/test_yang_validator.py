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
	# run validatin, exception raised if validation failed
    assert validator.validate(data, model_name="telecom-network") == True, "Validation failed"

# test_yang_validator_dict_data()
