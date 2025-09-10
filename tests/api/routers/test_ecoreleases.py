from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_ecoreleases(client: TestClient):
    utils.get_all_test(ep="/api/v1/ecoreleases", client=client)


def test_get_ecorelease(client: TestClient):
    instrument = utils.get_one_test(ep="/api/v1/ecoreleases/XTSBEZECI", client=client)
    assert instrument["instrument"] == "XTSBEZECI", (
        "The instrument should be XTSBEZECI."
    )


def test_get_ecorelease_not_found(client: TestClient):
    response = client.get("/api/v1/ecoreleases/whatever", headers=HEADERS)
    assert response.status_code == 404
