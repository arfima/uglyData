from fastapi.testclient import TestClient


def test_health(client: TestClient):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_db(client: TestClient):
    response = client.get("/health/db")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
