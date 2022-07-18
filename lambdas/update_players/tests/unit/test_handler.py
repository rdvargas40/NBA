import json
import pytest
from update_players import app

@pytest.fixture()
def apigw_event():
    """ Generates API GW Event"""

    return {
        "key1": 1,
        "key2": 2,
        "key3": 3
    }

def test_lambda_handler(apigw_event):

    ret = app.lambda_handler(apigw_event, "")
    data = json.loads(ret["body"])

    assert ret["statusCode"] == 200
    assert "message" in ret["body"]
    assert data["message"] == "Succeded"
    # assert "location" in data.dict_keys()
