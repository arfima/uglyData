from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_columns(client: TestClient):
    utils.get_all_test(ep="/api/v1/columns", client=client)


def test_get_all_columns_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/columns/count", client=client)


def test_get_column(client: TestClient):
    column = utils.get_one_test(ep="/api/v1/columns/test_column", client=client)
    assert column["column_name"] == "test_column"


def test_get_column_unauthorized(client: TestClient):
    response = client.get("/api/v1/columns/test_column")
    assert response.status_code == 401


def test_get_column_not_found(client: TestClient):
    response = client.get("/api/v1/columns/whatever", headers=HEADERS)
    assert response.status_code == 404


def test_post_column(client: TestClient):
    col = {
        "column_name": "test_column_2",
        "description": "Test column 2",
    }

    utils.post_test(
        ep="/api/v1/columns",
        client=client,
        item=col,
        check_ep="/api/v1/columns/test_column_2",
    )


def test_post_column_unauthorized(client: TestClient):
    col = {
        "column_name": "test_column_2",
        "description": "New description",
    }

    response = client.post("/api/v1/columns", json=col)
    assert response.status_code == 401


def test_put_column(client: TestClient):
    col = {
        "column_name": "test_column",
        "description": "Description modified",
    }

    utils.put_test(
        ep="/api/v1/columns",
        client=client,
        item=col,
        check_ep="/api/v1/columns/test_column",
    )


def test_put_column_unauthorized(client: TestClient):
    p = {
        "column_name": "test_column",
        "description": "Description modified 2",
    }

    response = client.put("/api/v1/columns", json=p)
    assert response.status_code == 401


def test_delete_column(client: TestClient):
    p = {
        "column_name": "test_column",
    }

    utils.delete_test(
        ep="/api/v1/columns",
        client=client,
        item=p,
        check_ep="/api/v1/columns/ICE",
    )


def test_delete_column_unauthorized(client: TestClient):
    p = {
        "column_name": "test_column",
    }

    response = client.request("DELETE", "/api/v1/columns", json=p)
    assert response.status_code == 401
