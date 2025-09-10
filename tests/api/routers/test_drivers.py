from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_drivers(client: TestClient):
    utils.get_all_test(ep="/api/v1/drivers", client=client)


def test_get_all_drivers_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/drivers/count", client=client)


def test_get_all_drivers_unauthorized(client: TestClient):
    response = client.get("/api/v1/drivers")
    assert response.status_code == 401


def test_get_driver(client: TestClient):
    driver = utils.get_one_test(ep="/api/v1/drivers/USSOC11H", client=client)
    assert driver["driver"] == "USSOC11H", "The driver should be USSOC11H."


def test_get_driver_unauthorized(client: TestClient):
    response = client.get("/api/v1/drivers/USSOC11H")
    assert response.status_code == 401


def test_get_driver_not_found(client: TestClient):
    response = client.get("/api/v1/drivers/whatever", headers=HEADERS)
    assert response.status_code == 404


def test_post_driver(client: TestClient):
    driver = {
        "driver": "TEST_driver",
        "description": "Test driver.",
        "dtype": "eod",
        "tags": [],
        "legs": [
            {
                "weight": 100,
                "instrument": "USSOC11H",
                "attr": "settle",
                "roll_method": None,
            },
        ],
    }

    utils.post_test(
        ep="/api/v1/drivers",
        client=client,
        item=driver,
        check_ep="/api/v1/drivers/TEST_driver",
    )


def test_post_driver_unauthorized(client: TestClient):
    driver = {
        "driver": "TEST_driver",
        "description": "Test driver.",
        "tags": [],
        "legs": [
            {"weight": 100, "instrument": "USSOC11H", "attr": "settle"},
        ],
    }

    response = client.post("/api/v1/drivers", json=driver)
    assert response.status_code == 401


def test_put_driver(client: TestClient):
    driver = {
        "driver": "USSOC11H",
        "dtype": "eod",
        "description": "Description Changed",
        "tags": [],
        "legs": [
            {
                "weight": 100,
                "instrument": "USSOC11H",
                "attr": "settle",
                "roll_method": None,
            },
        ],
    }

    utils.put_test(
        ep="/api/v1/drivers",
        client=client,
        item=driver,
        check_ep="/api/v1/drivers/USSOC11H",
    )


def test_put_driver_unauthorized(client: TestClient):
    driver = {
        "driver": "USSOC11H",
        "dtype": "eod",
        "description": "Description Changed",
        "tags": [],
        "legs": [
            {"weight": 100, "instrument": "USSOC11H", "attr": "settle"},
        ],
    }

    response = client.put("/api/v1/drivers", json=driver)
    assert response.status_code == 401


def test_delete_driver(client: TestClient):
    driver = {
        "driver": "LOIS3M",
        "dtype": "eod",
        "description": "Description Changed",
        "tags": [],
        "legs": [
            {
                "weight": 100,
                "instrument": "USSOC11H",
                "attr": "trade_price",
                "roll_method": None,
            },
            {
                "weight": -100,
                "instrument": "LIBOR3MIDX",
                "attr": "settle",
                "roll_method": None,
            },
        ],
    }

    utils.delete_test(
        ep="/api/v1/drivers",
        client=client,
        item=driver,
        check_ep="/api/v1/drivers/LOIS3M",
    )


def test_delete_driver_unauthorized(client: TestClient):
    driver = {
        "driver": "USSOC11H",
        "description": "Description Changed",
        "tags": [],
        "legs": None,
    }

    response = client.request("DELETE", "/api/v1/drivers", json=driver)
    assert response.status_code == 401
