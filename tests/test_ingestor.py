import pytest
import pandas as pd

from uglyData.db.postgres import AsyncDB
from uglyData.ingestor import TSLib
from uglyData.ingestor import timescale as ts

from psycopg.errors import UndefinedTable, DuplicateObject, DataError
from decimal import Decimal

# !important
"""
The compression policy causes issues with tests. Explanation in this issue:
https://github.com/timescale/timescaledb-extras/issues/25. It's something that
should only affect a newly created database. For now, I have removed the
compression policy on hypertables, but it may be established in future SQL
scripts that are run when instantiating the database for the first time. If
that happens, we will need to remove the compression policy from here before
running the tests.

This PR should fix the issue too: https://github.com/timescale/timescaledb-extras/pull/26
"""


@pytest.fixture(scope="module")
async def ingestor(conn_url):
    tslib = TSLib()
    async with await tslib.connect(conninfo=conn_url) as ingestor:
        yield ingestor


@pytest.fixture(scope="module")
def df_base():
    dtimes = pd.date_range("2020-01-01", periods=5, freq="s")
    dtimes = pd.to_datetime(dtimes, utc=True)
    df = pd.DataFrame(
        {
            "dtime": dtimes,
            "instrument": "EDH23",
            "trade_price": Decimal(1.0),
            "trade_size": Decimal(1),
            "aggressor": "buy",
            "rectime": dtimes,
            "exch_trade_id": "1",
            "nanos": 1,
        }
    )
    return df


@pytest.fixture(scope="module")
def df_multiple_chunks():
    dtimes = pd.DatetimeIndex([], tz="UTC")
    for year in ["2021", "2022", "2023", "2025"]:
        dtimes = dtimes.union(pd.date_range(f"{year}-01-01", periods=5, freq="s"))
    dtimes = pd.to_datetime(dtimes, utc=True)

    df = pd.DataFrame(
        {
            "dtime": dtimes,
            "instrument": "EDH23",
            "trade_price": Decimal(10.0),
            "trade_size": Decimal(1),
            "aggressor": "buy",
            "rectime": dtimes,
            "exch_trade_id": "1",
            "nanos": 1,
        }
    )

    return df


async def test_connection(ingestor: TSLib):
    """Test that the ingestor is connected to the database"""
    assert isinstance(ingestor.conn, AsyncDB)
    assert await ingestor.conn.fetch("SELECT 1", output="json") == [{"?column?": 1}]


async def test_invalid_dataframe(ingestor: TSLib):
    """Test that an error is raised when an empty dataframe is passed"""
    with pytest.raises(ValueError):
        await ingestor.insert(df=pd.DataFrame(), table="eikon_t2trade")


async def test_invalid_table(ingestor: TSLib):
    """Test that an error is raised when a table is not found in the database"""
    df = pd.DataFrame({"col1": [1, 2, 3, 4, 5], "col2": [1, 2, 3, 4, 5]})
    with pytest.raises(UndefinedTable):
        await ingestor.insert(df=df, table="invalid_table")


async def test_invalid_value(ingestor: TSLib, df_base: pd.DataFrame):
    df = df_base.copy()
    df["trade_price"] = "dwdwd"  # invalid value: must be a string

    with pytest.raises(DataError):
        await ingestor.insert(df=df, table="eikon_t2trade")


# async def test_tz_naive_timestamps(ingestor: TSLib, df_base: pd.DataFrame):
#     dtimes = pd.date_range("2020-01-01", periods=5, freq="h")
#     df = df_base.copy()
#     df["dtime"] = dtimes

#     with pytest.raises(TypeError):
#         await ingestor.insert(df=df, table="eikon_t2trade")


async def test_basic_insert(ingestor: TSLib, df_base: pd.DataFrame):
    df = df_base.copy()
    await ingestor.insert(df=df, table="eikon_t2trade", schema="primarydata")

    actual = await ingestor.conn.fetch(
        "SELECT * FROM primarydata.eikon_t2trade", output="dataframe"
    )
    assert pd.testing.assert_frame_equal(df, actual) is None


async def test_basic_insert_csv(ingestor: TSLib, df_base: pd.DataFrame):
    await ingestor.conn.execute("DELETE FROM primarydata.eikon_t2trade;")

    df = df_base.copy()
    csv = df.to_csv(index=False, header=False, sep="\t")

    await ingestor.insert(csv=csv, table="eikon_t2trade", schema="primarydata")

    actual = await ingestor.conn.fetch(
        "SELECT * FROM primarydata.eikon_t2trade", output="dataframe"
    )
    assert pd.testing.assert_frame_equal(df, actual) is None
    await ingestor.conn.execute("DELETE FROM primarydata.eikon_t2trade;")


async def test_basic_insert_csv_some_columns(ingestor: TSLib, df_base: pd.DataFrame):
    await ingestor.conn.execute("DELETE FROM primarydata.eikon_t2trade;")

    columns = ["dtime", "instrument", "exch_trade_id", "trade_price"]

    df = df_base.copy()
    df = df[columns]
    csv = df.to_csv(index=False, header=False, sep="\t")

    await ingestor.insert(
        csv=csv, columns=columns, table="eikon_t2trade", schema="primarydata"
    )

    actual = await ingestor.conn.fetch(
        "SELECT * FROM primarydata.eikon_t2trade", output="dataframe"
    )

    assert pd.testing.assert_frame_equal(df, actual[columns]) is None
    for col in [col for col in df.columns if col not in columns]:
        assert actual[col].isna().all()
    await ingestor.conn.execute("DELETE FROM primarydata.eikon_t2trade;")


async def test_insert_with_uniqueviolation_do_nothing(
    ingestor: TSLib, df_base: pd.DataFrame
):
    """
    Test that when a unique constraint is violated, the "do_nothing" conflict resolution
    strategy is correctly used and no rows are updated in the database
    """

    df = df_base.copy()

    await ingestor.insert(df=df, table="eikon_t2trade", schema="primarydata")

    actual = await ingestor.conn.fetch(
        "SELECT * FROM primarydata.eikon_t2trade", output="dataframe"
    )

    df_altered = df.copy()
    df_altered["trade_price"] = Decimal(2.0)

    await ingestor.insert(
        df=df_altered,
        table="eikon_t2trade",
        schema="primarydata",
        on_conflict="do_nothing",
    )

    actual = await ingestor.conn.fetch(
        "SELECT * FROM primarydata.eikon_t2trade", output="dataframe"
    )

    assert pd.testing.assert_frame_equal(df, actual) is None


async def test_insert_with_uniqueviolation_do_update(
    ingestor: TSLib, df_base: pd.DataFrame
):
    """
    Test that when a unique constraint is violated, the "do_update" conflict resolution
    strategy is correctly used and the rows are updated in the database
    """
    df = df_base.copy()
    await ingestor.insert(df=df, table="eikon_t2trade", schema="primarydata")

    df_altered = df.copy()
    df_altered["trade_price"] = Decimal(2.0)

    await ingestor.insert(
        df=df_altered, table="eikon_t2trade", schema="primarydata", on_conflict="update"
    )

    actual = await ingestor.conn.fetch(
        "SELECT * FROM primarydata.eikon_t2trade", output="dataframe"
    )

    assert pd.testing.assert_frame_equal(df_altered, actual) is None


async def test_insert_in_compressed_chunk_basic(ingestor: TSLib, df_base: pd.DataFrame):
    """Test that we can insert into a compressed chunk"""

    # compress chunk
    selected_chunk = await ingestor.conn.fetchval(
        "select show_chunks('primarydata.eikon_t2trade')"
    )
    try:
        await ingestor.conn.execute("select compress_chunk(%s)", (selected_chunk,))
    except DuplicateObject:
        pass  # chunk already compressed
    chunks = await ts.get_chunks(ingestor.conn, "eikon_t2trade", "primarydata")

    assert chunks["compression_status"][0] == "Compressed"

    df = df_base.copy()
    df["instrument"] = "TUH23"  # to not conflict with existing data from previous tests

    await ingestor.insert(df=df, table="eikon_t2trade", schema="primarydata")

    actual = await ingestor.conn.fetch(
        """
            SELECT * FROM primarydata.eikon_t2trade
            where instrument='TUH23'
        """,
        output="dataframe",
    )

    assert pd.testing.assert_frame_equal(df, actual) is None


async def test_insert_in_compressed_chunk_uniqueviolation_do_nothing(
    ingestor: TSLib, df_base: pd.DataFrame
):
    """Test that we can insert into a compressed chunk"""

    # compress chunk
    selected_chunk = await ingestor.conn.fetchval(
        "select show_chunks('primarydata.eikon_t2trade')"
    )
    try:
        await ingestor.conn.execute("select compress_chunk(%s)", (selected_chunk,))
    except DuplicateObject:
        pass  # chunk already compressed

    chunks = await ts.get_chunks(ingestor.conn, "eikon_t2trade", "primarydata")

    assert chunks["compression_status"][0] == "Compressed"

    df = df_base.copy()
    df["instrument"] = "TUH23"  # to not conflict with existing data from previous tests

    await ingestor.insert(df=df, table="eikon_t2trade", schema="primarydata")

    df_altered = df.copy()
    df_altered["trade_price"] = Decimal(20.0)

    await ingestor.insert(
        df=df_altered,
        table="eikon_t2trade",
        schema="primarydata",
        on_conflict="do_nothing",
    )

    actual = await ingestor.conn.fetch(
        """
            SELECT * FROM primarydata.eikon_t2trade
            where instrument='TUH23'
        """,
        output="dataframe",
    )

    assert pd.testing.assert_frame_equal(df, actual) is None


async def test_insert_in_compressed_chunk_uniqueviolation_update(
    ingestor: TSLib, df_base: pd.DataFrame
):
    """Test that we can insert into a compressed chunk"""

    # compress chunk
    selected_chunk = await ingestor.conn.fetchval(
        "select show_chunks('primarydata.eikon_t2trade')"
    )
    try:
        await ingestor.conn.execute("select compress_chunk(%s)", (selected_chunk,))
    except DuplicateObject:
        pass  # chunk already compressed

    chunks = await ts.get_chunks(ingestor.conn, "eikon_t2trade", "primarydata")

    assert chunks["compression_status"][0] == "Compressed"

    df = df_base.copy()
    df["instrument"] = "TUH23"  # to not conflict with existing data from previous tests

    await ingestor.insert(df=df, table="eikon_t2trade", schema="primarydata")

    df_altered = df.copy()
    df_altered["trade_price"] = Decimal(20.0)

    await ingestor.insert(
        df=df_altered, table="eikon_t2trade", schema="primarydata", on_conflict="update"
    )

    actual = await ingestor.conn.fetch(
        """
            SELECT * FROM primarydata.eikon_t2trade
            where instrument='TUH23'
        """,
        output="dataframe",
    )

    assert pd.testing.assert_frame_equal(df_altered, actual) is None


async def test_insert_multiple_compressed_chunks_do_nothing(
    ingestor: TSLib, df_multiple_chunks: pd.DataFrame
):
    """Test that we can insert into multiple compressed chunks"""

    # insert in multiple chunks

    df = df_multiple_chunks.copy()
    await ingestor.insert(df=df, table="eikon_t2trade", schema="primarydata")

    # compress all chunks: except one

    chunks = await ts.get_chunks(
        ingestor.conn, "eikon_t2trade", "primarydata", output="json"
    )

    for i, chunk in enumerate(chunks):
        if i == 3:
            continue  # skip one chunk
        chunk_name = f"{chunk['chunk_schema']}.{chunk['chunk_name']}"
        if chunk["compression_status"] == "Uncompressed":
            await ingestor.conn.execute("select compress_chunk(%s)", (chunk_name,))

    # insert again with altered values and do nothing

    df_altered = df.copy()
    df_altered["trade_price"] = Decimal(20.0)

    await ingestor.insert(df=df_altered, table="eikon_t2trade", schema="primarydata")

    actual = await ingestor.conn.fetch(
        """
            SELECT * FROM primarydata.eikon_t2trade
            where instrument='EDH23' and dtime >= '2021-01-01' and dtime < '2026-01-01'
        """,
        output="dataframe",
    )

    assert pd.testing.assert_frame_equal(df, actual) is None

    # do update

    # check chunks are still compressed

    chunks = await ts.get_chunks(
        ingestor.conn, "eikon_t2trade", "primarydata", output="json"
    )

    # we expect all chunks to be compressed except the one we skipped before
    expected = ["Compressed", "Compressed", "Compressed", "Uncompressed", "Compressed"]
    actual = [chunk["compression_status"] for chunk in chunks]

    assert [chunk["compression_status"] for chunk in chunks] == expected


async def test_insert_multiple_compressed_chunks_update(
    ingestor: TSLib, df_multiple_chunks: pd.DataFrame
):
    """Test that we can upser into multiple compressed chunks"""
    df_altered = df_multiple_chunks.copy()
    df_altered["trade_price"] = Decimal(20.5)

    await ingestor.insert(
        df=df_altered, table="eikon_t2trade", schema="primarydata", on_conflict="update"
    )

    actual = await ingestor.conn.fetch(
        """
            SELECT * FROM primarydata.eikon_t2trade
            where instrument='EDH23' and dtime >= '2021-01-01' and dtime < '2026-01-01'
        """,
        output="dataframe",
    )

    assert pd.testing.assert_frame_equal(df_altered, actual) is None


async def test_insert_force_nulls_enabled(ingestor: TSLib, df_base: pd.DataFrame):
    """Test that we can insert with on_conflict=update and force_nulls=True"""
    df = df_base.copy()

    await ingestor.insert(df=df, table="eikon_t2trade", schema="primarydata")

    df_altered = df.copy()
    df_altered["aggressor"] = "NULL"

    await ingestor.insert(
        df=df_altered,
        table="eikon_t2trade",
        schema="primarydata",
        on_conflict="update",
        force_nulls=True,
    )

    actual = await ingestor.conn.fetch(
        """
        SELECT * FROM primarydata.eikon_t2trade 
        WHERE instrument='EDH23' and exch_trade_id='1' and aggressor='NULL'""",
        output="dataframe",
    )

    assert pd.testing.assert_frame_equal(df_altered, actual) is None


@pytest.mark.skip(
    reason="changes in latest timescaledb versions make obsolete the setting."
)
async def test_insert_recompress_disabled(ingestor: TSLib, df_base: pd.DataFrame):
    """Test that we can insert with on_conflict=update and recompress=False"""
    df = df_base.copy()

    await ingestor.insert(df=df, table="eikon_t2trade", schema="primarydata")

    # compress chunk
    selected_chunk = await ingestor.conn.fetchval(
        "select show_chunks('primarydata.eikon_t2trade')"
    )
    try:
        await ingestor.conn.execute("select compress_chunk(%s)", (selected_chunk,))
    except DuplicateObject:
        pass  # chunk already compressed

    # check that the chunck is compressed
    chunks = await ts.get_chunks(ingestor.conn, "eikon_t2trade", "primarydata")
    assert chunks["compression_status"][2] == "Compressed"

    df_altered = df.copy()
    df_altered["aggressor"] = "SUU"

    await ingestor.insert(
        df=df_altered,
        table="eikon_t2trade",
        schema="primarydata",
        on_conflict="update",
        recompress_after=False,
    )

    # check that the chunck is not compressed
    chunks = await ts.get_chunks(ingestor.conn, "eikon_t2trade", "primarydata")
    assert chunks["compression_status"][2] == "Uncompressed"

    actual = await ingestor.conn.fetch(
        """ 
            SELECT * FROM primarydata.eikon_t2trade 
            WHERE instrument='EDH23' and exch_trade_id='1' and aggressor='SUU'""",
        output="dataframe",
    )

    assert pd.testing.assert_frame_equal(df_altered, actual) is None
