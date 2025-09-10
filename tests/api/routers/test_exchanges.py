from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_exchanges(client: TestClient):
    utils.get_all_test(ep="/api/v1/exchanges", client=client)


def test_get_all_exchanges_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/exchanges/count", client=client)


def test_get_all_exchanges_unauthorized(client: TestClient):
    response = client.get("/api/v1/exchanges")
    assert response.status_code == 401


def test_get_exchange(client: TestClient):
    exchange = utils.get_one_test(ep="/api/v1/exchanges/CME", client=client)
    assert exchange["mic"] == "CME", "The exchange MIC should be CME."


def test_get_exchange_unauthorized(client: TestClient):
    response = client.get("/api/v1/exchanges/CME")
    assert response.status_code == 401


def test_get_exchange_not_found(client: TestClient):
    response = client.get("/api/v1/exchanges/whatever", headers=HEADERS)
    assert response.status_code == 404


def test_post_exchange(client: TestClient):
    e = {
        "mic": "TEST_exchange",
        "description": "Test exchange.",
        "url": "https://www.cmegroup.com",
        "arfimaname": "TEST",
        "comment": "Test exchange.",
        "tt_ticker": "TEST",
    }

    utils.post_test(
        ep="/api/v1/exchanges",
        client=client,
        item=e,
        check_ep="/api/v1/exchanges/TEST_exchange",
    )


def test_post_exchange_unauthorized(client: TestClient):
    e = {
        "mic": "TEST_exchange",
        "description": "Test exchange.",
        "url": "https://www.cmegroup.com",
        "arfimaname": "TEST",
        "comment": "Test exchange.",
        "tt_ticker": "TEST",
    }

    response = client.post("/api/v1/exchanges", json=e)
    assert response.status_code == 401


def test_put_exchange(client: TestClient):
    p = {
        "mic": "CME",
        "description": "Description modified",
    }

    utils.put_test(
        ep="/api/v1/exchanges",
        client=client,
        item=p,
        check_ep="/api/v1/exchanges/CME",
    )


def test_put_exchange_unauthorized(client: TestClient):
    p = {
        "mic": "CME",
        "description": "Description modified",
    }

    response = client.put("/api/v1/exchanges", json=p)
    assert response.status_code == 401


def test_delete_exchange(client: TestClient):
    p = {
        "mic": "ICE",
        "description": "Intercontinental Exchange",
    }

    utils.delete_test(
        ep="/api/v1/exchanges",
        client=client,
        item=p,
        check_ep="/api/v1/exchanges/ICE",
    )


def test_delete_exchange_unauthorized(client: TestClient):
    p = {
        "mic": "ICE",
        "description": "Intercontinental Exchange",
    }

    response = client.request("DELETE", "/api/v1/exchanges", json=p)
    assert response.status_code == 401
