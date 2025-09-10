import pandas as pd

from uglyData.db import AsyncDB
from uglyData.api.builders import (
    BuilderFactory,
    StandardBuilder,
    DriverBuilder,
    SpreadBuilder,
    StrategyBuilder,
)
from uglyData.api.models import LoadRequest
from uglyData.api.service import DatabaseService


async def test_builder_factory_standard(db: AsyncDB):
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="EDM22",
        dtype="eod",
        from_date="2023-01-01",
        to_date="2023-01-31",
    )

    builder = await BuilderFactory.get_builder(request=request, db=db)
    assert isinstance(builder, StandardBuilder)


async def test_builder_factory_standard_filters_min_max(db: AsyncDB):
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="SX5EIDX*",
        dtype="opteod",
        from_date="2020-01-01",
        to_date="2020-12-01",
        filters={"strike": {"min": 3000, "max": 5000}},
    )

    builder = await BuilderFactory.get_builder(request=request, db=db)
    assert isinstance(builder, StandardBuilder)

    sql = await builder.build_sql()
    data = await db.conn.fetch(sql, output="dataframe")
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 0
    assert data["strike"].min() >= 3000
    assert data["strike"].max() <= 5000


async def test_builder_factory_standard_filters_list(db: AsyncDB):
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="SX5EIDX*",
        dtype="opteod",
        from_date="2020-01-01",
        to_date="2020-12-01",
        filters={"type": ["W", "D"]},
    )

    builder = await BuilderFactory.get_builder(request=request, db=db)
    assert isinstance(builder, StandardBuilder)

    sql = await builder.build_sql()
    data = await db.conn.fetch(sql, output="dataframe")
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 0
    assert data["type"].isin(["W", "D"]).all()


async def test_builder_factory_driver(db: AsyncDB):
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="USSOC11H",  # this ticker must be in the drivers table
        dtype="eod",
        from_date="2023-01-01",
        to_date="2023-01-31",
    )
    builder = await BuilderFactory.get_builder(request=request, db=db)
    assert isinstance(builder, DriverBuilder)


async def test_builder_factory_spread(db: AsyncDB):
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="US-UK TSY 10 H25ext",  # this ticker must be in the drivers table
        dtype="spreadeod",
        from_date="2023-01-01",
        to_date="2023-01-31",
    )
    builder = await BuilderFactory.get_builder(request=request, db=db)
    assert isinstance(builder, SpreadBuilder)


async def test_builder_factory_strategy(db: AsyncDB):
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="CROSS%",  # this ticker must be in the drivers table
        dtype="strategieseod",
        from_date="2023-01-01",
        to_date="2023-01-31",
    )
    builder = await BuilderFactory.get_builder(request=request, db=db)
    assert isinstance(builder, StrategyBuilder)


async def test_generic_builder_sql(db: AsyncDB):
    """TODO: This can be improved using mock data. This only tests that the query is
    working."""
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="EDG1NR",
        dtype="eod",
        from_date="2023-01-01",
        to_date="2023-01-31",
    )
    builder = await BuilderFactory.get_builder(request=request, db=db)
    sql = await builder.build_sql()
    r = await db.conn.fetch(sql, output="dataframe")
    assert isinstance(r, pd.DataFrame)


async def test_driver_builder_sql(db: AsyncDB):
    """TODO: This can be improved using mock data. This only tests that the query is
    working."""
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="TESTDRIVER",
        dtype="eod",
        from_date="2023-01-01",
        to_date="2023-01-31",
    )
    builder = await BuilderFactory.get_builder(request=request, db=db)
    sql = await builder.build_sql()
    r = await db.conn.fetch(sql, output="dataframe")
    assert isinstance(r, pd.DataFrame)


async def test_driver_spread_sql(db: AsyncDB):
    """TODO: This can be improved using mock data. This only tests that the query is
    working."""
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="TESTSPREAD",
        dtype="spreadeod",
        from_date="2023-01-01",
        to_date="2023-01-31",
    )
    builder = await BuilderFactory.get_builder(request=request, db=db)
    sql = await builder.build_sql()
    r = await db.conn.fetch(sql, output="dataframe")
    assert isinstance(r, pd.DataFrame)


async def test_driver_strategy_sql(db: AsyncDB):
    """TODO: This can be improved using mock data. This only tests that the query is
    working."""
    db = DatabaseService(db=db)
    request = LoadRequest(
        ticker="TESTSTRATEGY",
        dtype="strategiesintra",
        from_date="2023-01-01",
        to_date="2023-01-31",
    )
    builder = await BuilderFactory.get_builder(request=request, db=db)
    sql = await builder.build_sql()
    r = await db.conn.fetch(sql, output="dataframe")
    assert isinstance(r, pd.DataFrame)
