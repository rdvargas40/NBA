from functions.update_players import app


def test_update_players():
    input_payload = {"key": 1}

    players = app.lambda_handler(input_payload, "")
    
    assert all([p['is_active'] for p in players])

