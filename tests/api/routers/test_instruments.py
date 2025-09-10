from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_instruments(client: TestClient):
    utils.get_all_test(ep="/api/v1/instruments", client=client)


def test_get_all_instruments_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/instruments/count", client=client)


def test_get_all_instruments_unauthorized(client: TestClient):
    response = client.get("/api/v1/instruments")
    assert response.status_code == 401


def test_get_instrument(client: TestClient):
    instrument = utils.get_one_test(ep="/api/v1/instruments/EDH23", client=client)
    assert instrument["instrument"] == "EDH23", "The instrument should be EDH23."


def test_get_instrument_not_found(client: TestClient):
    response = client.get("/api/v1/instruments/whatever", headers=HEADERS)
    assert response.status_code == 404


def test_post_instrument(client: TestClient):
    instrument = {
        "instrument": "TEST_instrument",
        "product": "ED",
        "product_type": "Outright",
        "refinitiv_ticker": None,
        "bloomberg_ticker": None,
    }

    utils.post_test(
        ep="/api/v1/instruments",
        client=client,
        item=instrument,
        check_ep="/api/v1/instruments/TEST_instrument",
    )


def test_post_instrument_unauthorized(client: TestClient):
    instrument = {
        "instrument": "TEST_instrument",
        "product": "ED",
        "product_type": "Outright",
    }

    response = client.post("/api/v1/instruments", json=instrument)
    assert response.status_code == 401


def test_put_instrument(client: TestClient):
    instrument = {
        "instrument": "EDH23",
        "product": "ED",
        "product_type": "Outright",
        "first_tradeable_date": "2023-01-01",
    }

    utils.put_test(
        ep="/api/v1/instruments",
        client=client,
        item=instrument,
        check_ep="/api/v1/instruments/EDH23",
    )


def test_put_instrument_unauthorized(client: TestClient):
    instrument = {
        "instrument": "EDH23",
        "product": "ED",
        "product_type": "Outright",
        "first_tradeable_date": "2023-01-01",
    }

    response = client.put("/api/v1/instruments", json=instrument)
    assert response.status_code == 401


def test_delete_instrument(client: TestClient):
    instrument = {
        "instrument": "FFZ22F23",
        "product": "FF",
        "product_type": "Calendar",
    }

    utils.delete_test(
        ep="/api/v1/instruments",
        client=client,
        item=instrument,
        check_ep="/api/v1/instruments/FFZ22F23",
    )


def test_delete_instrument_unauthorized(client: TestClient):
    instrument = {
        "instrument": "EDH23",
        "product": "ED",
        "product_type": "Outright",
    }

    response = client.request("DELETE", "/api/v1/instruments", json=instrument)
    assert response.status_code == 401


def test_get_instrument_deliverables(client: TestClient):
    deliverabels = utils.get_all_test(
        ep="/api/v1/instruments/deliverables/?instrument=TUH23&instrument=TUM24",
        client=client,
    )
    assert deliverabels[0]["instrument"] == "TUH23", "The instrument should be TUH23."


def test_get_instrument_last_cheapest(client: TestClient):
    cheapest = utils.get_all_test(
        ep="/api/v1/instruments/last_cheapest/?instrument=TUH23",
        client=client,
    )
    assert isinstance(cheapest, list), "The response should be a dictionary."
    assert isinstance(cheapest[0], dict), "The response should be a dictionary."
    assert cheapest[0]["instrument"] == "TUH23", "The instrument should be TUH23."
    assert cheapest[0]["isin"] is not None, "Cheapest should not be None"


def test_get_instrument_cheapest_fixed(client: TestClient):
    cheapest_fixed = utils.get_all_test(
        ep="/api/v1/instruments/cheapest_fixed/?instrument=TUH23", client=client
    )
    assert isinstance(cheapest_fixed, list), "The response should be a dictionary."
    assert isinstance(cheapest_fixed[0], dict), "The response should be a dictionary."
    assert cheapest_fixed[0]["instrument"] == "TUH23", "The instrument should be TUH23."
    assert cheapest_fixed[0]["isin"] is not None, "Cheapest should not be None"
