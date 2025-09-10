import logging
import traceback
import io

import elasticapm
import pandas as pd
from fastapi import WebSocket, WebSocketDisconnect, HTTPException, WebSocketException


from decimal import Decimal

from .service import DB, ts_lib
from uglyData.api.models import User, AccessLevel
from .dependencies import check_permissions, request_handler
from .auth import websocket_auth

LOG = logging.getLogger("uvicorn")


class Message:
    ACK = "ack"
    AUTH_FAILED = "auth_failed"


def decode_parquet(df_bytes):
    return pd.read_parquet(io.BytesIO(df_bytes))


async def load_websocket(
    dtype: str,
    websocket: WebSocket,
    endpoint_name: str,
    model_cls,
    drop_cols: list[str] = [],
    chunk_size: int = 10000,
    dropna_cols=True,
):
    """Wait for a request, then stream the data requested to the client using the
    the given websocket. Data is streamed as Pandas DataFrame by chunks in parquet
    bytes.

    Parameters
    ----------
    source : str
        First part of the table name
    tabtype : str
        Second part of the table name, e.g. "t2tquote", "t2ttrade"
    websocket : WebSocket
        Websocket connection to use for streaming.
    endpoint_name : str
        Name of the endpoint, e.g. "/api/v1/market/". Just for logging the transaction.
    model_cls : pydantic.BaseModel
        Pydantic model to use for validate the request.
    drop_cols : list[str]
        List of columns to drop from the response dataframe.
    chunk_size : int, optional
        Size of the chunks to send, by default 10000


    """
    elasticapm.instrument()
    client = elasticapm.get_client()

    t = client.begin_transaction(transaction_type="request")
    if t:
        t.name = f"WS {endpoint_name}{dtype}"

    await websocket.accept()
    try:
        request = await websocket.receive_json()
        request["dtype"] = dtype
        host = websocket.client.host if websocket.client else "unknown"
        LOG.info(f"Request received from {host}", extra=request)
        elasticapm.label(dtype=dtype, instrument=request["ticker"])
        elasticapm.set_context(request)
        request = model_cls(**request)

        pages = DB.paginate_market_data(
            request=request,
            page_size=chunk_size,
            output="dataframe",
            return_count=True,
        )

        count = await pages.__anext__()
        await websocket.send_json(count)

        async for df in pages:
            for drop_col in drop_cols:
                if drop_col in df.columns:  # instrument selected in the request
                    df.drop(drop_col, axis=1, inplace=True)
            if dropna_cols:
                df = df.dropna(axis=1, how="all")
            if "dtime" in df.columns:
                df = df.set_index("dtime")
            size = df.shape[0]
            with elasticapm.capture_span("build parquet"):
                # ! Temporal fix while deciding what to do with infinities
                df = df.replace([Decimal("Infinity"), Decimal("-Infinity")], None)
                df = df.to_parquet()
            with elasticapm.capture_span("send data"):
                await websocket.send_bytes(df)
            LOG.debug("Send data block", extra={"size": size})

        client.end_transaction("websocket_endpoint", "SUCCESS")
        await websocket.close()
    except WebSocketDisconnect:
        print("client disconnected")
        client.end_transaction("websocket", "FAILURE")


async def handle_load_websocket(
    dtype: str, websocket: WebSocket, endpoint: str, model_cls, **kwargs
):
    """Wait for a request, then stream data back to the client using
    websockets. Data is streamed as Pandas DataFrame of 10000 rows at
    a time in parquet bytes."""
    try:
        user = await websocket_auth(websocket.headers)
        LOG.info(f'User "{user.username}" connected to websocket')
        with request_handler(user=user, table="market", access_level=AccessLevel.READ):
            return await load_websocket(dtype, websocket, endpoint, model_cls, **kwargs)
    except Exception as e:
        LOG.error(f"Error in websocket: {e}")
        await websocket.send_text(str(e))
        await websocket.receive_json()
        raise WebSocketException(code=1011)


async def handle_store_websocket(
    table: str, websocket: WebSocket, endpoint: str, user: User, schema: str
):
    """Wait for a request, then store the data that the client sends"""
    elasticapm.instrument()
    client = elasticapm.get_client()
    t = client.begin_transaction(transaction_type="store")
    if t:
        t.name = f"WS {endpoint}"
    await websocket.accept()
    try:
        if not await _check_store_auth(table, websocket, user):
            return
        elasticapm.label(table=table)
        while True:
            data = await websocket.receive_bytes()
            with elasticapm.capture_span("build parquet"):
                df = decode_parquet(data)
            with elasticapm.capture_span("store and send ack"):
                exc = await _store(table, df, schema)
            LOG.debug("Received data block", extra={"size": df.shape[0]})
            await websocket.send_text(Message.ACK if exc is None else exc)

    except WebSocketDisconnect:
        print("client disconnected")
        client.end_transaction("websocket", "FAILURE")
    client.end_transaction("websocket_endpoint", "SUCCESS")


async def _store(table: str, df: pd.DataFrame, schema: str):
    """Store df in the DB using TSLib"""
    try:
        await ts_lib.insert(
            table,
            schema,
            df,
            on_conflict="update",
            force_nulls=False,
            recompress_after=True,
            drop_duplicates=False,
        )  # Warning: this insert does not throw an error when duplicate elements are
        # stored. It ignores it by default
    except Exception:
        LOG.debug(f"Exception in tslib: {traceback.format_exc()}")
        return traceback.format_exc()
    # EXCEPTIONS to check: psycopg.errors.NumericValueOutOfRange,
    # psycopg.errors.BadCopyFileFormat
    return None


async def _check_store_auth(table: str, websocket: WebSocket, user: User):
    try:
        if user:
            check_permissions(user, table, AccessLevel.WRITE)
    except HTTPException as e:
        LOG.error(f"Auth failed for table {table}: {e}")
        await websocket.send_text(Message.AUTH_FAILED)
        await websocket.close()
        return False
    await websocket.send_text(Message.ACK)
    return True
