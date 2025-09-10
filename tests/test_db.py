from uglyData.db.postgres import AsyncDB, AsyncDBPool
import pandas as pd


async def test_connect(db: AsyncDB):
    assert await db.fetch("SELECT 1", output="json") == [{"?column?": 1}]


async def test_connect_pool(conn_url):
    async with AsyncDBPool() as pool:
        async with await pool.connect(conninfo=conn_url) as db:
            assert await db.fetch("SELECT 1")


async def test_execute(db: AsyncDB):
    await db.execute("SELECT 1")


async def test_fetch(db: AsyncDB):
    assert await db.fetch("SELECT 1", output="json") == [{"?column?": 1}]


async def test_fetchval(db: AsyncDB):
    assert await db.fetchval("SELECT 1") == 1


async def test_get_columns(db: AsyncDB):
    # fmt: off
    expected = set(['dtime', 'instrument', 'trade_price', 'trade_size',
                'aggressor', 'rectime', 'exch_trade_id', 'nanos'])

    actual = set(await db.get_columns("eikon_t2trade", "primarydata"))

    assert actual == expected


async def test_get_pkeys(db: AsyncDB):
    # fmt: off
    expected = set(['dtime', 'instrument','exch_trade_id'])

    actual = set(await db.get_pkeys("eikon_t2trade", "primarydata"))

    assert actual == expected


async def test_copy_to_table(db: AsyncDB):
    await db.execute(
        """ CREATE TABLE test_table (
            dtime timestamp, value int )
        """
    )
    df = pd.DataFrame(
        {
            "dtime": pd.date_range("2020-01-01", periods=100, freq="D"),
            "value": range(100),
        }
    )

    await db.copy_to_table(df=df, table_name="test_table")

    rows = await db.fetchval("SELECT COUNT(*) FROM test_table")
    assert rows == 100


async def test_paginate(db: AsyncDB):
    sql = "SELECT * FROM test_table"
    n_pages = 0
    async for page in db.paginate(sql, page_size=10):
        n_pages += 1
        assert len(page) <= 10
    assert n_pages == 10
