import pandas as pd
from uglyData.api.models import LoadRequest
from datetime import datetime
from uglyData.api.service import DatabaseService
from tests.conftest import AsyncDB


async def test_paginate_market_data_freq(conn_url):
    """Test that freq paramater aggregate data in bars"""
    DB = DatabaseService(db=AsyncDB())
    request = LoadRequest(
        ticker="EDM22",
        dtype="quotes",
        from_date=datetime(2019, 1, 1),
        to_date=datetime(2021, 1, 31),
        freq="1d",
    )

    await DB.connect(conn_url)

    await DB.conn.execute(
        """ CREATE TABLE testss_t2t (
            dtime timestamp with time zone, instrument text,bid_price0 decimal, 
            bid_size0 decimal, bid_orders0 int, ask_price0 decimal, ask_size0 decimal, 
            ask_orders0 int, rectime timestamptz,quote_id text, nanos int,
            PRIMARY KEY (dtime,instrument, quote_id) )
        """
    )

    dtimes = pd.date_range("2020-01-01", periods=5, freq="h")
    dtimes = pd.to_datetime(dtimes, utc=True)

    df = pd.DataFrame(
        {
            "dtime": dtimes,
            "instrument": "EDM22",
            "bid_price0": 0,
            "bid_size0": 0,
            "bid_orders0": 0,
            "ask_price0": 0,
            "ask_size0": 0,
            "ask_orders0": 0,
            "rectime": dtimes,
            "quote_id": "1",
            "nanos": 0,
        }
    )

    await DB.conn.copy_to_table(df=df, table_name="primarydata.refinitiv_t2tquote")

    rows = len(df)

    assert rows == 5
    agg_rows = 0
    async for df in DB.paginate_market_data(
        page_size=10,
        request=request,
        output="dataframe",
    ):
        agg_rows += len(df)

    assert agg_rows == 1
