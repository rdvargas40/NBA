import json
import pytest
from update_teams_matchs_s3 import app

@pytest.fixture()
def apigw_event():
    """ Generates API GW Event"""

    return {
        "team_id": 1610612747,
        "abbreviation": "LAL"
    }

def test_lambda_handler(apigw_event):

    ret = app.lambda_handler(apigw_event, "")
    data = json.loads(ret["body"])

    assert ret["statusCode"] == 200
    assert "message" in ret["body"]
    assert  "Succeded" in data["message"]
    # assert "location" in data.dict_keys()
