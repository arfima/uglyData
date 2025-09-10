from fastapi.testclient import TestClient
import utils

HEADERS = {"Authorization": "Bearer 1234"}


def test_get_all_products(client: TestClient):
    utils.get_all_test(ep="/api/v1/products", client=client)


def test_get_all_products_count(client: TestClient):
    utils.get_all_count_test(ep="/api/v1/products/count", client=client)


# Disabled temporarily because Audit trail pipeline doesn't use authentication yet
# and need to use /products endpoint
# def test_get_all_products_unauthorized(client: TestClient):
#     response = client.get("/api/v1/products")
#     assert response.status_code == 401


def test_get_product(client: TestClient):
    product = utils.get_one_test(ep="/api/v1/products/Outright/ED", client=client)
    assert product["product"] == "ED", "The product should be ED."
    assert product["product_type"] == "Outright", "The product type should be Outright."


# def test_get_product_unauthorized(client: TestClient):
#     response = client.get("/api/v1/products/Outright/ED")
#     assert response.status_code == 401


def test_get_product_not_found(client: TestClient):
    response = client.get("/api/v1/products/Outright/whatever", headers=HEADERS)
    assert response.status_code == 404


def test_post_product(client: TestClient):
    p = {
        "product": "TEST_PRODUCT",
        "product_type": "Outright",
        "family": "Fixed Income",
        "description": "Test product.",
        "subfamily": "STIRS",
        "exchange": "CME",
    }

    utils.post_test(
        ep="/api/v1/products",
        client=client,
        item=p,
        check_ep="/api/v1/products/Outright/TEST_PRODUCT",
    )


def test_post_product_unauthorized(client: TestClient):
    p = {
        "product": "TEST_PRODUCT",
        "product_type": "Outright",
        "family": "Fixed Income",
        "description": "Test product.",
        "subfamily": "STIRS",
        "exchange": "CME",
    }

    response = client.post("/api/v1/products", json=p)
    assert response.status_code == 401


def test_put_product(client: TestClient):
    p = {
        "product": "ED",
        "product_type": "Outright",
        "description": "Description modified",
    }

    utils.put_test(
        ep="/api/v1/products",
        client=client,
        item=p,
        check_ep="/api/v1/products/Outright/ED",
    )


def test_put_product_unauthorized(client: TestClient):
    p = {
        "product": "ED",
        "product_type": "Outright",
        "description": "Description modified",
    }

    response = client.put("/api/v1/products", json=p)
    assert response.status_code == 401


def test_delete_product(client: TestClient):
    p = {
        "product": "NG",
        "product_type": "Outright",
    }

    utils.delete_test(
        ep="/api/v1/products",
        client=client,
        item=p,
        check_ep="/api/v1/products/Outright/NG",
    )


def test_delete_product_unauthorized(client: TestClient):
    p = {
        "product": "NG",
        "product_type": "Outright",
    }

    response = client.request("DELETE", "/api/v1/products", json=p)
    assert response.status_code == 401
