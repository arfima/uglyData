from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_events(client: TestClient):
    utils.get_all_test(ep="/api/v1/events", client=client)


def test_get_all_events_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/events/count", client=client)


def test_get_all_events_unauthorized(client: TestClient):
    response = client.get("/api/v1/events")
    assert response.status_code == 401


def test_get_event(client: TestClient):
    event = utils.get_one_test(ep="/api/v1/events/1", client=client)
    assert event["id"] == 1, "The event id should be 1."


def test_get_event_unauthorized(client: TestClient):
    response = client.get("/api/v1/events/1")
    assert response.status_code == 401


def test_get_event_not_found(client: TestClient):
    response = client.get("/api/v1/events/123123123", headers=HEADERS)
    assert response.status_code == 404


def test_post_event(client: TestClient):
    event = {
        "description": "Test event2.",
        "event_category": "Test",
        "start_date": "2020-01-01",
        "end_date": "2020-01-01",
    }

    utils.post_test(
        ep="/api/v1/events",
        client=client,
        item=event,
        check_ep="/api/v1/events/3",
    )


def test_post_event_unauthorized(client: TestClient):
    event = {
        "description": "Test event.",
        "event_category": "Test",
        "start_date": "2020-01-01",
        "end_date": "2020-01-01",
    }

    response = client.post("/api/v1/events", json=event)
    assert response.status_code == 401


def test_put_event(client: TestClient):
    event = {
        "id": 1,
        "description": "description changed",
        "event_category": "Test",
        "start_date": "2020-01-01",
        "end_date": "2020-01-01",
    }

    utils.put_test(
        ep="/api/v1/events",
        client=client,
        item=event,
        check_ep="/api/v1/events/1",
    )


def test_put_event_unauthorized(client: TestClient):
    event = {
        "id": 1,
        "description": "description changed",
        "event_category": "Test",
        "start_date": "2020-01-01",
        "end_date": "2020-01-01",
    }

    response = client.put("/api/v1/events", json=event)
    assert response.status_code == 401


def test_delete_event(client: TestClient):
    event = {
        "id": 2,
        "description": "Example event",
        "event_category": "CategoryB",
        "start_date": "2021-01-01",
        "end_date": "2021-01-01",
    }

    utils.delete_test(
        ep="/api/v1/events",
        client=client,
        item=event,
        check_ep="/api/v1/events/2",
    )


def test_delete_event_unauthorized(client: TestClient):
    event = {
        "id": 2,
        "description": "Example event",
        "event_category": "CategoryB",
        "start_date": "2021-01-01",
        "end_date": "2021-01-01",
    }

    response = client.request("DELETE", "/api/v1/events", json=event)
    assert response.status_code == 401
