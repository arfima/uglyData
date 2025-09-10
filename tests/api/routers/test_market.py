from fastapi.testclient import TestClient
import pandas as pd
import io

HEADERS = {"Authorization": "Bearer 1234"}


def parquet_bytes_to_df(df_bytes):
    return pd.read_parquet(io.BytesIO(df_bytes))


def test_ws_load_t2t_quotes(client: TestClient):
    with client.websocket_connect("/api/v1/market/t2t/quotes", headers=HEADERS) as ws:
        ws.send_json({"ticker": "EDH23", "dtype": "quotes"})
        _ = ws.receive_json()  # first message is the count
        data = ws.receive_bytes()
    df = parquet_bytes_to_df(data)
    assert df.shape[0] > 0


def test_ws_load_t2t_trades(client: TestClient):
    with client.websocket_connect("/api/v1/market/t2t/trades", headers=HEADERS) as ws:
        ws.send_json({"ticker": "EDH23", "dtype": "trades"})
        _ = ws.receive_json()
        data = ws.receive_bytes()
    df = parquet_bytes_to_df(data)
    assert df.shape[0] > 0


def test_ws_load_eod(client: TestClient):
    HEADERS = {"Authorization": "Bearer 1234"}

    with client.websocket_connect("/api/v1/market/eod", headers=HEADERS) as ws:
        ws.send_json({"ticker": "EDH23", "dtype": "eod"})
        _ = ws.receive_json()
        data = ws.receive_bytes()
    df = parquet_bytes_to_df(data)
    assert df.shape[0] > 0


def test_ws_load_events(client: TestClient):
    HEADERS = {"Authorization": "Bearer 1234"}

    with client.websocket_connect("/api/v1/market/events", headers=HEADERS) as ws:
        ws.send_json({"ticker": None, "dtype": "events", "filters": {}})
        _ = ws.receive_json()
        data = ws.receive_bytes()
    df = parquet_bytes_to_df(data)
    assert df.shape[0] > 0
