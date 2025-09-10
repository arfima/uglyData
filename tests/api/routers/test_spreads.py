from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_spreads(client: TestClient):
    utils.get_all_test(ep="/api/v1/spreads", client=client)


def test_get_all_spreads_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/spreads/count", client=client)


def test_get_all_spreads_unauthorized(client: TestClient):
    response = client.get("/api/v1/spreads")
    assert response.status_code == 401


def test_get_spread(client: TestClient):
    spread = utils.get_one_test(ep="/api/v1/spreads/spread1", client=client)
    assert spread["arfima_name"] == "spread1", "The spread should be spread1."


def test_get_spread_unauthorized(client: TestClient):
    response = client.get("/api/v1/spreads/spread1")
    assert response.status_code == 401


def test_get_spread_not_found(client: TestClient):
    response = client.get("/api/v1/spreads/whatever", headers=HEADERS)
    assert response.status_code == 404


def test_post_spread(client: TestClient):
    spread = {
        "arfima_name": "TEST_SPREAD",
        "executions": [
            {
                "legs": [
                    {
                        "instrument": "EDM22",
                        "weight_spread": 1000,
                        "weight_price": 10,
                        "weight_yield": 100,
                    },
                    {
                        "instrument": "EDH23",
                        "weight_spread": 100,
                        "weight_price": 10,
                        "weight_yield": 100,
                    },
                ],
            },
        ],
    }

    utils.post_test(
        ep="/api/v1/spreads",
        client=client,
        item=spread,
        check_ep="/api/v1/spreads/TEST_SPREAD",
    )


def test_post_spread_unauthorized(client: TestClient):
    spread = {
        "spread": "TEST_spread",
        "description": "Test spread.",
        "tags": [],
        "legs": [
            {"weight": 100, "instrument": "USSOC11H", "attr": "settle"},
        ],
    }

    response = client.post("/api/v1/spreads", json=spread)
    assert response.status_code == 401


def test_put_spread(client: TestClient):
    spread = {
        "arfima_name": "spread2",
        "executions": [
            {
                "legs": [
                    {
                        "instrument": "EDM22",
                        "weight_spread": 1,
                        "weight_price": 10,
                        "weight_yield": 100,
                    },
                    {
                        "instrument": "EDH23",
                        "weight_spread": 100,
                        "weight_price": 10,
                        "weight_yield": 100,
                    },
                ],
            },
            {
                "legs": [
                    {"instrument": "EDM22", "weight_price": 50},
                    {"instrument": "EDH23", "weight_price": 100},
                ]
            },
        ],
    }

    utils.put_test(
        ep="/api/v1/spreads",
        client=client,
        item=spread,
        check_ep="/api/v1/spreads/spread2",
    )


def test_put_spread_unauthorized(client: TestClient):
    spread = {
        "spread": "USSOC11H",
        "dtype": "eod",
        "description": "Description Changed",
        "tags": [],
        "legs": [
            {"weight": 100, "instrument": "USSOC11H", "attr": "settle"},
        ],
    }

    response = client.put("/api/v1/spreads", json=spread)
    assert response.status_code == 401


def test_delete_spread(client: TestClient):
    spread = {
        "arfima_name": "spread2",
        "executions": [
            {
                "legs": [
                    {
                        "instrument": "EDM22",
                        "weight_spread": 1,
                        "weight_price": 10,
                        "weight_yield": 100,
                    },
                    {
                        "instrument": "EDH23",
                        "weight_spread": 100,
                        "weight_price": 10,
                        "weight_yield": 100,
                    },
                ],
            },
            {
                "legs": [
                    {"instrument": "EDM22", "weight_price": 50},
                    {"instrument": "EDH23", "weight_price": 100},
                ]
            },
        ],
    }

    utils.delete_test(
        ep="/api/v1/spreads",
        client=client,
        item=spread,
        check_ep="/api/v1/spreads/spread2",
    )


def test_delete_spread_unauthorized(client: TestClient):
    spread = {
        "spread": "USSOC11H",
        "description": "Description Changed",
        "tags": [],
        "legs": None,
    }

    response = client.request("DELETE", "/api/v1/spreads", json=spread)
    assert response.status_code == 401
