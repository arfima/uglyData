from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_users(client: TestClient):
    utils.get_all_test(ep="/api/v1/users", client=client)


def test_get_all_users_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/users/count", client=client)


def test_get_all_users_unauthorized(client: TestClient):
    response = client.get("/api/v1/users")
    assert response.status_code == 401


def test_get_user(client: TestClient):
    user = utils.get_one_test(ep="/api/v1/users/test1", client=client)
    assert user["username"] == "test1", "The user should be test1."


def test_get_user_unauthorized(client: TestClient):
    response = client.get("/api/v1/users/test1")
    assert response.status_code == 401


def test_get_user_not_found(client: TestClient):
    response = client.get("/api/v1/users/123123123", headers=HEADERS)
    assert response.status_code == 404


def test_post_user(client: TestClient):
    user = {
        "username": "new_user",
        "name": "Test user",
    }

    utils.post_test(
        ep="/api/v1/users",
        client=client,
        item=user,
        check_ep="/api/v1/users/new_user",
    )


def test_post_user_unauthorized(client: TestClient):
    user = {
        "username": "new_user",
        "name": "Test user",
    }

    response = client.post("/api/v1/users", json=user)
    assert response.status_code == 401


def test_put_user(client: TestClient):
    user = {
        "username": "test1",
        "name": "New name",
    }

    utils.put_test(
        ep="/api/v1/users",
        client=client,
        item=user,
        check_ep="/api/v1/users/test1",
    )


def test_put_user_unauthorized(client: TestClient):
    user = {
        "username": "test1",
        "name": "New name",
    }

    response = client.put("/api/v1/users", json=user)
    assert response.status_code == 401
