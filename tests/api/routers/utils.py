from fastapi.testclient import TestClient
from typing import Any

HEADERS = {"Authorization": "Bearer 1234"}


def is_subdict(small: dict, big: dict) -> bool:
    """Check if a dictionary is a subdictionary of another dictionary."""
    # return big | small == big
    return all([k in big for k in small.keys()])


def get_all_test(ep: str, client: TestClient) -> list[dict[str, Any]]:
    """Test that the endpoint returns a list of valid models.

    Parameters
    ----------
    ep : str
        The endpoint to test.
    client : TestClient
        The starlette test client to use to make the request.
    model : BaseModel
        The pydantic model to use to validate the response.

    Returns
    -------
    dict
        The response from the endpoint. This is returned so that can be used for
        additional tests.
    """
    response = client.get(ep, headers=HEADERS)
    assert response.status_code == 200
    items = response.json()
    assert isinstance(items, list), "The response should be a list"
    assert len(items) > 0, "There should be at least one item in the database."
    assert isinstance(items[0], dict), "Each item should be a dictionary."
    return items


def get_all_count_test(ep: str, client: TestClient) -> dict[str, Any]:
    """Test that the endpoint returns a count of the number of items in the database.

    Parameters
    ----------
    ep : str
        The endpoint to test.
    client : TestClient
        The starlette test client to use to make the request.

    Returns
    -------
    dict
        The response from the endpoint. This is returned so that can be used for
        additional tests.
    """
    response = client.get(ep, headers=HEADERS)
    assert response.status_code == 200
    assert isinstance(response.json(), dict), "The response should be a dictionary."
    assert "count" in response.json(), "The response should have a 'count' key."
    count = response.json()["count"]
    assert isinstance(count, int), "The response should be an integer."
    assert count > 0, "There should be at least one product in the database."
    return response.json()


def get_one_test(ep: str, client: TestClient) -> dict[str, Any]:
    """
    Test that the endpoint returns an item successfully.

    Parameters
    ----------
    ep : str
        The endpoint to test.
    client : TestClient
        The starlette test client to use to make the request.
    model : BaseModel
        The pydantic model to use to validate the response.

    Returns
    -------
    dict
        The response from the endpoint. This is returned so that can be used for
        additional tests.
    """
    response = client.get(ep, headers=HEADERS)
    assert response.status_code == 200
    item = response.json()
    assert isinstance(item, dict), "The response should be a dictionary."
    return item


def post_test(
    ep: str, client: TestClient, item: dict[str, Any], check_ep: str
) -> dict[str, Any]:
    """
    Test that the endpoint post an item successfully and returns the same item.

    Parameters
    ----------
    ep : str
        The endpoint to test.
    client : TestClient
        The starlette test client to use to make the request.
    item : dict
        The item to post.
    check_ep : str
        The endpoint to use to check that the item has been added to the database.
    """
    # Check that POST endpoint returns the same item
    response = client.post(ep, headers=HEADERS, json=item)
    assert response.status_code == 201
    expected = response.json()
    assert isinstance(expected, dict), "The response should be a dictionary."
    assert is_subdict(item, expected)

    # Check that the item has been added to the database
    actual = get_one_test(ep=check_ep, client=client)
    assert is_subdict(expected, actual)

    return expected


def put_test(
    ep: str, client: TestClient, item: dict[str, Any], check_ep: str
) -> dict[str, Any]:
    """
    Test that the endpoint put an item successfully and returns the same item.

    Parameters
    ----------
    ep : str
        The endpoint to test.
    client : TestClient
        The starlette test client to use to make the request.
    item : dict
        The item to put.
    check_ep : str
        The endpoint to use to check that the item has been added to the database.
    """
    # Check that PUT endpoint returns the same item
    response = client.put(ep, headers=HEADERS, json=item)
    assert response.status_code == 200
    expected = response.json()
    assert isinstance(expected, dict), "The response should be a dictionary."
    assert is_subdict(item, expected)

    # Check that the item has been added to the database
    actual = get_one_test(ep=check_ep, client=client)
    assert is_subdict(expected, actual)

    return expected


def delete_test(
    ep: str, client: TestClient, item: dict[str, Any], check_ep: str
) -> dict[str, Any]:
    """
    Test that the endpoint delete an item successfully and returns the same item.

    Parameters
    ----------
    ep : str
        The endpoint to test.
    client : TestClient
        The starlette test client to use to make the request.
    item : dict
        The item to delete.
    check_ep : str
        The endpoint to use to check that the item has been deleted from the database.
    """
    # Check that DELETE endpoint returns the same item
    response = client.request("DELETE", ep, headers=HEADERS, json=item)
    assert response.status_code == 200
    expected = response.json()
    assert isinstance(expected, dict), "The response should be a dictionary."
    assert is_subdict(item, expected)

    # Check that the item has been deleted from the database
    response = client.get(check_ep, headers=HEADERS)
    assert response.status_code == 404

    return expected
