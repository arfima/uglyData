from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_families(client: TestClient):
    utils.get_all_test(ep="/api/v1/families", client=client)


def test_get_all_families_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/families/count", client=client)


def test_get_all_families_unauthorized(client: TestClient):
    response = client.get("/api/v1/families")
    assert response.status_code == 401


def test_get_family(client: TestClient):
    family = utils.get_one_test(ep="/api/v1/families/Indices", client=client)
    assert family["family"] == "Indices", "The family should be Indices."


def test_get_family_unauthorized(client: TestClient):
    response = client.get("/api/v1/families/STIRS")
    assert response.status_code == 401


def test_get_family_not_found(client: TestClient):
    response = client.get("/api/v1/families/whatever", headers=HEADERS)
    assert response.status_code == 404


def test_post_family(client: TestClient):
    fam = {
        "family": "TEST_family",
        "description": "Test family.",
    }

    utils.post_test(
        ep="/api/v1/families",
        client=client,
        item=fam,
        check_ep="/api/v1/families/TEST_family",
    )


def test_post_family_unauthorized(client: TestClient):
    fam = {
        "family": "TEST_family",
        "description": "Test family.",
    }

    response = client.post("/api/v1/families", json=fam)
    assert response.status_code == 401


def test_put_family(client: TestClient):
    fam = {
        "family": "Indices",
        "description": "Description modified",
    }

    utils.put_test(
        ep="/api/v1/families",
        client=client,
        item=fam,
        check_ep="/api/v1/families/Indices",
    )


def test_put_family_unauthorized(client: TestClient):
    fam = {
        "family": "Indices",
        "description": "Description modified",
    }

    response = client.put("/api/v1/families", json=fam)
    assert response.status_code == 401


def test_delete_family(client: TestClient):
    fam = {
        "family": "Currencies",
        "description": "Currency futures.",
    }

    utils.delete_test(
        ep="/api/v1/families",
        client=client,
        item=fam,
        check_ep="/api/v1/families/Currencies",
    )


def test_delete_family_unauthorized(client: TestClient):
    fam = {
        "family": "Currencies",
    }

    response = client.request("DELETE", "/api/v1/families", json=fam)
    assert response.status_code == 401
