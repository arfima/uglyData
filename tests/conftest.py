import sys
import pytest
from testcontainers.postgres import PostgresContainer
from uglyData.db.postgres import AsyncDB
from fastapi.testclient import TestClient
from app import app
import psycopg
import os
import asyncio
import pandas as pd

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

IMAGE = "timescale/timescaledb:2.17.2-pg17"
SQL_SCRIPTS_PATH = "uglyData/db/scripts/"


@pytest.fixture(scope="session")
def conn_url():
    postgres = PostgresContainer(IMAGE, user="dbadm")

    if sys.platform.startswith("win"):
        postgres.get_container_host_ip = lambda: "localhost"
    # |_> windows fix: https://github.com/testcontainers/testcontainers-python/issues/108#issuecomment-768367971

    with postgres as psql:
        url = psql.get_connection_url()
        url = url.replace("+psycopg2", "")
        with psycopg.connect(url) as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            sql_scripts = os.listdir(SQL_SCRIPTS_PATH)
            for script in sql_scripts:
                with open(SQL_SCRIPTS_PATH + script, "r") as f:
                    conn.execute(f.read())
        yield url


# @pytest.fixture(scope="session")
# def event_loop():
#     import sys

#     if sys.platform == "win32":
#         asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
#     loop = asyncio.get_event_loop()
#     yield loop
#     loop.close()


@pytest.fixture(scope="session")
async def db(conn_url):
    async with await AsyncDB().connect(conninfo=conn_url) as db:
        yield db


class MockResponse:
    def __init__(self, json, status):
        self._json = json
        self.status = status

    async def json(self):
        return self._json

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self

    def raise_for_status(self):
        pass


def get_user_override(*args, **kwargs):
    return MockResponse(json={"username": "test1"}, status=200)


@pytest.fixture(scope="function", autouse=True)
def mock_get_user(monkeypatch):
    monkeypatch.setattr("aiohttp.ClientSession.get", get_user_override)


@pytest.fixture(scope="session")
def set_env():
    os.environ["API_DB_SERVICE"] = "dbalfa9"


@pytest.fixture(scope="session")
def client(conn_url):
    os.environ["API_DB_CONN_INFO"] = conn_url
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    with TestClient(app) as client:
        yield client


async def fake_get_product(product):
    return {
        "listed_contracts_letters": "HMUZ",
        "listed_contracts": 40,
        "listed_contracts_offcycle_letters": "FGJKNQVX",
        "listed_contracts_offcycle": 4,
        "bloomberg_ticker": product,
        "bloomberg_suffix": "Comdty",
    }


async def fake_get_generics(self):
    generics_dict = {
        "roll_period_start": {0: "2023-01-01 00:00:00"},
        "roll_period_end": {0: "2023-01-31 00:00:00"},
        "g1": {0: "EDG23"},
    }
    df = pd.DataFrame(generics_dict)
    df["roll_period_start"] = pd.to_datetime(df["roll_period_start"])
    df["roll_period_end"] = pd.to_datetime(df["roll_period_end"])
    return df
