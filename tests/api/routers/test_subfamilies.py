from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_subfamilies(client: TestClient):
    utils.get_all_test(ep="/api/v1/subfamilies", client=client)


def test_get_all_subfamilies_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/subfamilies/count", client=client)


def test_get_all_subfamilies_unauthorized(client: TestClient):
    response = client.get("/api/v1/subfamilies")
    assert response.status_code == 401


def test_get_subfamily(client: TestClient):
    subfamily = utils.get_one_test(ep="/api/v1/subfamilies/STIRS", client=client)
    assert subfamily["subfamily"] == "STIRS", "The subfamily should be STIRS."


def test_get_subfamily_unauthorized(client: TestClient):
    response = client.get("/api/v1/subfamilies/STIRS")
    assert response.status_code == 401


def test_get_subfamily_not_found(client: TestClient):
    response = client.get("/api/v1/subfamilies/whatever", headers=HEADERS)
    assert response.status_code == 404


def test_post_subfamily(client: TestClient):
    fam = {
        "subfamily": "TEST_subfamily",
        "family": "Fixed Income",
        "description": "Test subfamily.",
    }

    utils.post_test(
        ep="/api/v1/subfamilies",
        client=client,
        item=fam,
        check_ep="/api/v1/subfamilies/TEST_subfamily",
    )


def test_post_subfamily_unauthorized(client: TestClient):
    fam = {
        "subfamily": "TEST_subfamily",
        "family": "Fixed Income",
        "description": "Test subfamily.",
    }

    response = client.post("/api/v1/subfamilies", json=fam)
    assert response.status_code == 401


def test_put_subfamily(client: TestClient):
    fam = {
        "subfamily": "STIRS",
        "family": "Fixed Income",
        "description": "Description modified",
    }

    utils.put_test(
        ep="/api/v1/subfamilies",
        client=client,
        item=fam,
        check_ep="/api/v1/subfamilies/STIRS",
    )


def test_put_subfamily_unauthorized(client: TestClient):
    fam = {
        "subfamily": "Indices",
        "family": "Fixed Income",
        "description": "Description modified",
    }

    response = client.put("/api/v1/subfamilies", json=fam)
    assert response.status_code == 401


def test_delete_subfamily(client: TestClient):
    fam = {
        "subfamily": "Dividends",
        "family": "Commodities",
        "description": "Dividend futures.",
    }

    utils.delete_test(
        ep="/api/v1/subfamilies",
        client=client,
        item=fam,
        check_ep="/api/v1/subfamilies/Dividends",
    )


def test_delete_subfamily_unauthorized(client: TestClient):
    fam = {"subfamily": "Dividends", "family": "Commodities"}

    response = client.request("DELETE", "/api/v1/subfamilies", json=fam)
    assert response.status_code == 401
