"""Module with the classes and functions for constructing the sql queries."""

from __future__ import annotations

import datetime as dt
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import pandas as pd
from async_lru import alru_cache
from uglyData.api.models import Driver, LoadRequest

from ..api.exceptions import DataTypeNotFound

if TYPE_CHECKING:
    from .service import DatabaseService


PRODTYPE_REGEX = {
    "FSW": r"(?P<product>\w+)F(?P<tenor1>\d+)(?P<tenorUnit1>[DWMY])(?P<tenor2>\d+)(?P<tenorUnit2>[DWMY])SW",  # noqa
    "GNR": r"(?P<product>\w+)G(?P<generic>(?:\d+|\w)(?:_\d+|\w){0,2})NR",
    "IDX": r"(?P<product>\w+)IDX",
    "EQY": r"(?P<product>\w+)EQY",
    "PDX": r"(?P<product>(?:EC\w+)|(?:PRIT|RCPP)|(?:(?:US|CA|EZ)0[AB]NM))P(?:(?:(?<!PRITP|RCPPP)(?P<quarter>Q[1-4])?(?P<month>[FGHJKMNQUVXZ])?(?P<period>\d{4}))|(?:(?P<code>.*)))DX",  # noqa
    "TDX": r"(?P<product>\w+)T(?P<tenor>\d+[DWMY](?:_\d+[DWMY])?)DX",
    "TGN": r"(?P<product>\w+)T(?P<tenor>\d+[DWMY](?:_\d+[DWMY])?)(?P<generic>(?:\d+|\w)+)GN",  # noqa
    "Option": r"(?P<underlying>\w+)(?P<callput>C|P)(?P<maturity>\d{8})(?P<optionType>[DWMEY])(?P<strike>\d+(?:_\d+)?)",  # noqa
}
"""Regular expression patterns for the different product types (e.g. PDX)."""


AGG_FUNCS = {
    "quotes": {
        "instrument": "instrument",
        "bid_open": "first(bid_price0, dtime order by dtime)",
        "bid_high": "max(bid_price0)",
        "bid_low": "min(bid_price0)",
        "bid_close": "last(bid_price0, dtime order by dtime)",
        "ask_open": "first(ask_price0, dtime order by dtime)",
        "ask_high": "max(ask_price0)",
        "ask_low": "min(ask_price0)",
        "ask_close": "last(ask_price0, dtime order by dtime)",
        "ticks": "count(*)",
    },
    "intraquote": {
        "instrument": "instrument",
        "bid_open": "first(bid_open, dtime order by dtime)",
        "bid_high": "max(bid_high)",
        "bid_low": "min(bid_low)",
        "bid_close": "last(bid_close, dtime order by dtime)",
        "ask_open": "first(ask_open, dtime order by dtime)",
        "ask_high": "max(ask_high)",
        "ask_low": "min(ask_low)",
        "ask_close": "last(ask_close, dtime order by dtime)",
        "ticks": "sum(ticks)",
    },
}

AUDIT_SCHEMA = "tradingdata"
LIMIT_DATA = 10_000
TABLES = {
    "eod": "primarydata.base_eod",
    "intraquote": "primarydata.base_intraquote",
    "intrade": "primarydata.base_intrade",
    "quotes": "primarydata.refinitiv_t2tquote",
    "trades": "primarydata.refinitiv_t2trade",
    "opteod": "primarydata.base_opteod",
    "optintraquote": "primarydata.base_optintraquote",
    "tt_audittrail": "tradingdata.tt_audittrail_view",
    "dlveod": "secondarydata.base_dlveod",
    "dlvintra": "secondarydata.base_dlvintra",
    "usa_auctions": "primarydata.usa_auctions",
    "ger_auctions": "primarydata.ger_auctions",
    "fed_holdings": "primarydata.fed_holdings",
    "fed_purchases": "primarydata.fed_purchases",
    "events": "info.events",
    "spreadeod": "secondarydata.base_spreadeod",
    "spreadintra": "secondarydata.base_spreadintra",
    "strategieseod": "secondarydata.base_strategieseod",
    "strategiesintra": "secondarydata.base_strategiesintra",
    "ecoreleases": "primarydata.eikon_ecoreleases",
}


def _merge_prods(row: pd.Series, product: str, generics: list[int]) -> str:
    lp = len(product)
    name = f"{product}{''.join([row[f'g{gn}'][lp:] for gn in generics])}"
    return name


def get_timestamp_column(dtype: str):
    ts_cols = {
        "usa_auctions": "auction_date",
        "ger_auctions": "auction_date",
        "fed_purchases": "dtime0",
    }
    if dtype in ts_cols:
        return ts_cols[dtype]
    else:
        return "dtime"


CUSTOM_INDEXES_TABLE = "secondarydata.cus_indexes_{dtype}"

AGG_TABLES = {
    "0 days 00:01:00": "primarydata.base_{dtype}",
    "0 days 00:30:00": "primarydata.t30_{dtype}",
}


def sql_to_agg(sql: str, dtype: str, freq: pd.Timedelta) -> str:
    """Return the aggregation query for the given SQL query. Which will be turned into
    a subquery."""
    time_col = get_timestamp_column(dtype)
    freq = str(freq)
    if freq in AGG_TABLES and dtype in ["intraquote", "intrade"]:
        table = AGG_TABLES[freq].format(dtype=dtype)
        return sql.replace(f"FROM {TABLES[dtype]}", f"FROM {table}")
    else:
        table = TABLES[dtype]

        sql_agg = f""" SELECT 
            time_bucket('{freq}', {time_col}) AS {time_col},
        """
        if dtype not in AGG_FUNCS:
            raise ValueError(
                f"Cannot aggregate '{dtype}' data, only {AGG_FUNCS.keys()}"
            )
        cols_agg = AGG_FUNCS[dtype]
        for col, agg in cols_agg.items():
            sql_agg += f"{agg} AS {col}, "
        sql_agg = sql_agg[:-2]  # remove last comma
        sql_agg += f"""
            FROM ({sql}) as _data
            GROUP BY 1,instrument
        """
        return sql_agg


class CurveNotAvailable(Exception):
    pass


class Builder(ABC):
    """Abstract class for building SQL queries for loading market data. The query is
    builted based on the request content."""

    def __init__(self, request, db=None, table=None):
        self.request = request
        self.table = table
        if db is not None:
            self.db = db

    @abstractmethod
    async def build_sql(self) -> str:
        """Build SQL query for loading market data"""
        pass

    async def get_columns(self):
        if self.request.dtype == "eod":
            return await self._get_columns_eod()
        else:
            schema, table = self.table.split(".")
            return await self.db.conn.get_columns(table, schema=schema)


class StandardBuilder(Builder):
    """Standard builder for loading market data. This is the builder for standard
    instruments (i.e. not generic, not futures spreads, not drivers, etc.). It is
    selected as default when the request doesn't match any of the other builders."""

    async def build_sql(self):
        """Build SQL query for loading market data for a single instrument"""
        sql = f"SELECT * FROM {self.table} "
        if self.request.ticker is not None:
            if "*" in self.request.ticker:
                sql += f"""
                     WHERE instrument LIKE '{self.request.ticker.replace("*", "%")}' 
                """
            else:
                sql += f""" 
                     WHERE instrument = '{self.request.ticker}' 
                """

        from_date = self.request.from_date
        to_date = self.request.to_date

        if from_date is None or to_date is None:
            first_date, last_date = await self.get_available_range()
            from_date = from_date or first_date
            to_date = to_date or last_date

        ts_col = get_timestamp_column(self.request.dtype)

        if self.request.ticker is not None:
            sql += " AND "
        else:
            sql += " WHERE "

        sql += f"{ts_col} >= '{from_date}' "
        sql += f"AND {ts_col} <= '{to_date}' "

        if self.request.filters:
            for key, value in self.request.filters.items():
                sql += _filter_to_sql(key, value)
        # sql += " ORDER BY 1 asc "
        if self.request.dtype in ["quotes", "trades"]:
            if self.request.from_date is None or self.request.to_date is None:
                sql += f"LIMIT {LIMIT_DATA}"

        if self.request.freq:
            sql = sql_to_agg(sql, self.request.dtype, self.request.freq)

        return sql

    async def get_available_range(self) -> tuple[dt.datetime, dt.datetime]:
        """Return the first and last dates available for the requested instrument."""
        dtype = self.request.dtype
        instrument = self.request.ticker
        dates = await self.db.conn.select(
            table="primarydata.instruments_info",
            fields=["start", "end"],
            filters={"dtype": dtype, "instrument": instrument},
        )
        return dates[0] if dates else (None, None)

    async def _get_columns_eod(self):
        schema = self.table.split(".")[0]
        if schema == "primarydata":
            sql = f"""SELECT product from info.instruments 
                        where instrument = '{self.request.ticker}'"""
            try:
                product = await self.db.conn.fetchval(sql)
            except TypeError:  # instrument not found
                return []
            sql = f"""SELECT eod_columns from info.products 
                    where product = '{product}'"""
            columns = await self.db.conn.fetchval(sql)
        elif schema == "secondarydata":
            driver = await get_driver(self.request.ticker, self.db)
            if driver["legs"] is not None:
                return [driver["legs"][0]["attr"]]
            else:
                return []
        return columns


def _filter_to_sql(key, value):
    if isinstance(value, list):
        values = (
            "("
            + ", ".join([f"'{v}'" if isinstance(v, str) else f"{v}" for v in value])
            + ")"
        )
        query = f" AND {key} IN {values} "
    elif isinstance(value, dict):
        low = value["min"]
        low = f"'{low}'" if isinstance(low, str) else low
        high = value["max"]
        high = f"'{high}'" if isinstance(high, str) else high
        query = ""
        if low is not None:
            query += f"AND {key} >= {low} "
        if high is not None:
            query += f"AND {key} <= {high} "
    return query


class AggBuilder(Builder):
    """Build used for aggregating data. This is selected when the request has
    the ``freq`` parameter."""

    async def build_sql(self):
        tabtype = self.request.dtype
        freq = self.request.freq
        if freq and tabtype not in AGG_FUNCS:
            raise ValueError(
                f"Cannot aggregate '{tabtype}' data, only {AGG_FUNCS.keys()}"
            )

        try:
            freq = pd.Timedelta(freq)
        except ValueError:
            raise ValueError(f"Invalid frequency: {freq}")

        sql = f""" SELECT time_bucket('{freq}', dtime) AS dtime """
        for col, agg in AGG_FUNCS[tabtype].items():
            sql += f", {agg} AS {col}"

        sql += f"""
            FROM {self.table}
            WHERE instrument = '{self.request.ticker}' AND
            dtime >= '{self.request.from_date}' AND dtime <= '{self.request.to_date}'
            GROUP BY 1
        """

        return sql


class AuditTrailBuilder(Builder):
    """Builder for loading audit trail data. This is selected when the request has
    the ``dtype`` parameter set to ``tt_audittrail``."""

    async def build_sql(self):
        """Build SQL query for loading market data for a single instrument"""
        sql = f"""
            SELECT * FROM {self.table} 
        """
        sql_filters = []
        if self.request.from_date:
            sql_filters.append(f"dtime >= '{self.request.from_date}' ")
        if self.request.to_date:
            sql_filters.append(f"dtime <= '{self.request.to_date}' ")
        for attr in self.request.default_filter_attributes:
            try:
                if getattr(self.request, attr):
                    sql_filters.append(f"{attr} = '{getattr(self.request, attr)}' ")
            except AttributeError:
                pass
        if self.request.custom_filter:
            sql_filters.append(f" ({self.request.custom_filter}) ")
        if sql_filters:
            sql += "WHERE " + "AND ".join(sql_filters)

        # sql += f"ORDER BY dtime asc, df_row_id asc"
        if self.request.limit:
            sql += f" LIMIT {self.request.limit}"
        return sql


def fill_na(leg, dtimes) -> str:
    """Return the fill_na SQL code for the given leg."""
    sql = f"""
        fill_na({leg["instrument"]}.{leg["attr"]}) over (order by {dtimes})
    """
    return sql


class DriverBuilder(Builder):
    """
    Builder for loading drivers.

    It is selected when the ticker requested is a driver (i.e. it is in the drivers
    table in the database).

    This is just for the drivers that are linear combination of other instruments or
    drivers. It uses the properly builder recursively to build the subqueries and the
    joins them.
    """

    def __init__(
        self, request: LoadRequest, driver: dict, db, table: str = None, *args, **kwargs
    ):
        super().__init__(request, db=db, table=table, *args, **kwargs)
        self.driver = driver

    async def get_available_range(self):
        first_dates = []
        last_dates = []
        for leg in self.driver["legs"]:
            request = LoadRequest(
                ticker=leg["instrument"],
                from_date=self.request.from_date,
                to_date=self.request.to_date,
                dtype=self.request.dtype,
            )
            builder = await BuilderFactory.get_builder(request=request, db=self.db)
            first_date, last_date = await builder.get_available_range()
            first_dates.append(first_date)
            last_dates.append(last_date)
        return max(first_dates), min(last_dates)

    async def _build_linear_comb_sql(self):
        from_date = self.request.from_date
        to_date = self.request.to_date
        dtype = self.driver["dtype"]

        legs = self.driver["legs"]
        legs_dtimes = ", ".join([f"{leg['instrument']}.dtime" for leg in legs])
        dtimes = f"COALESCE({legs_dtimes})"
        linear_comb = " + ".join(
            [f"{leg['weight']} * {fill_na(leg, dtimes)}" for leg in legs]
        )
        first_leg = legs[0]["instrument"]
        query = f"""
            SELECT 
                {dtimes} as dtime,
                {linear_comb} as {legs[0]["attr"]} 
            FROM  
            """
        if from_date is None or to_date is None:
            first_date, last_date = await self.get_available_range()
            from_date = from_date or first_date
            to_date = to_date or last_date

        for i, leg in enumerate(legs):
            current_leg = leg["instrument"]
            request = LoadRequest(
                ticker=leg["instrument"],
                from_date=from_date,
                to_date=to_date,
                dtype=dtype,
            )

            builder = await BuilderFactory.get_builder(request=request, db=self.db)
            subquery = "("
            subquery += await builder.build_sql()
            subquery += f") {current_leg}"

            if i > 0:
                query += " FULL OUTER JOIN "
            query += subquery
            if i > 0:
                query += f" ON {first_leg}.dtime = {current_leg}.dtime "

        return query

    async def build_sql(self):
        if self.driver["legs"] is None:
            raise ValueError(
                f"Driver leg '{self.driver['driver']}' data not available."
            )
        return await self._build_linear_comb_sql()

    async def get_columns(self):
        if self.driver["legs"] is None:
            return []
        else:
            return [self.driver["legs"][0]["attr"]]


class DLVBuilder(Builder):
    """Builder for loading secondary data. This is selected when the request has
    the ``dtype`` parameter set to ``secondarydata``."""

    def __init__(
        self,
        request: LoadRequest,
        db: DatabaseService,
        product: str = None,
        generics: list[int] = None,
        table: str = None,
    ):
        super().__init__(request, db=db, table=table)
        self.generics = generics
        self.product = product
        self.roll_method = request.options.roll_method

    async def build_sql_specific(self):
        dtype_op = self.request.options
        cheapest_filter = dtype_op.cheapest_filter

        if cheapest_filter == "all":
            sql = f"""SELECT * FROM {self.table} dlv
                    WHERE instrument = '{self.request.ticker}' """
        else:
            if self.request.dtype == "dlvintra":
                sql = f"""SELECT dlv.* FROM {self.table} dlv
                        INNER JOIN primarydata.base_cheapest ctd 
                        ON dlv.instrument = ctd.instrument 
                        AND 
                            (CASE WHEN dlv.dtime::time BETWEEN '20:00:00' AND '23:59:59'
                                THEN dlv.dtime::date + 1
                                ELSE dlv.dtime::date
                            END) = ctd.dtime
                        AND dlv.deliverable_isin = ctd.{cheapest_filter}
                        WHERE dlv.instrument = '{self.request.ticker}'"""
            else:
                sql = f"""SELECT dlv.* FROM {self.table} dlv
                    INNER JOIN primarydata.base_cheapest ctd 
                    ON dlv.instrument = ctd.instrument 
                    AND dlv.dtime::date = ctd.dtime
                    AND dlv.deliverable_isin = ctd.{cheapest_filter}
                    WHERE dlv.instrument = '{self.request.ticker}'"""

        sql_filters = []
        if self.request.from_date:
            sql_filters.append(f"dlv.dtime >= '{self.request.from_date}' ")
        if self.request.to_date:
            sql_filters.append(f"dlv.dtime <= '{self.request.to_date}' ")

        if sql_filters:
            sql += " AND " + "AND ".join(sql_filters)

        if self.request.filters:
            for key, value in self.request.filters.items():
                sql += _filter_to_sql(key, value)

        return sql

    async def build_sql(self):
        """Build SQL query for loading secondary data for a single instrument"""

        return await self.build_sql_specific()


class SpreadBuilder(Builder):
    """Builder for loading secondary data. This is selected when the request has
    the ``dtype`` parameter set to ``spreadeod`` or ``spreadintra``."""

    async def build_sql(self):
        """Build SQL query for loading secondary data for spreads starting by a prefix."""

        dtype_op = self.request.options

        sql = f"""SELECT * FROM {self.table} spreads
                    WHERE spreads.spread LIKE '{self.request.ticker}'"""

        sql_filters = []
        if self.request.from_date:
            sql_filters.append(f"spreads.dtime >= '{self.request.from_date}' ")
        if self.request.to_date:
            sql_filters.append(f"spreads.dtime <= '{self.request.to_date}' ")

        if sql_filters:
            sql += " AND " + "AND ".join(sql_filters)

        if self.request.filters:
            for key, value in self.request.filters.items():
                sql += _filter_to_sql(key, value)

        return sql


class StrategyBuilder(Builder):
    """Builder for loading secondary data. This is selected when the request has
    the ``dtype`` parameter set to ``strategies``."""

    async def build_sql(self):
        """Build SQL query for loading secondary data for strategy starting by a prefix."""

        dtype_op = self.request.options

        sql = f"""SELECT * FROM {self.table} strategies
                    WHERE strategies.strategy LIKE '{self.request.ticker}'
                    OR strategies.generic_strategy LIKE '{self.request.ticker}'
                """

        sql_filters = []
        if self.request.from_date:
            sql_filters.append(f"strategies.dtime >= '{self.request.from_date}' ")
        if self.request.to_date:
            sql_filters.append(f"strategies.dtime <= '{self.request.to_date}' ")

        if sql_filters:
            sql += " AND " + "AND ".join(sql_filters)

        if self.request.filters:
            for key, value in self.request.filters.items():
                sql += _filter_to_sql(key, value)

        return sql


class EcoReleaseBuilder(Builder):
    """Builder for loading primary data. This is selected when the request has
    the ``dtype`` parameter set to ``ecoreleases``."""

    async def build_sql(self):
        """Build SQL query for loading primary data for an eco release starting by a prefix."""

        dtype_op = self.request.options

        sql = f"""SELECT * FROM {self.table} ecorelease
                    WHERE ecorelease.instrument LIKE '{self.request.ticker}'"""

        sql_filters = []
        if self.request.from_date:
            sql_filters.append(f"ecorelease.dtime >= '{self.request.from_date}' ")
        if self.request.to_date:
            sql_filters.append(f"ecorelease.dtime <= '{self.request.to_date}' ")

        if sql_filters:
            sql += " AND " + "AND ".join(sql_filters)

        if self.request.filters:
            for key, value in self.request.filters.items():
                sql += _filter_to_sql(key, value)

        return sql


class EventBuilder(Builder):
    """Builder for loading secondary data. This is selected when the request has
    the ``dtype`` parameter set to ``secondarydata``."""

    async def build_sql(self):
        """Build SQL query for loading secondary data for a single instrument"""

        event_columns = [
            "id",
            "start_date",
            "start_dt",
            "end_date",
            "end_dt",
            "event_name",
            "event_category",
            "event_subcategory",
            "description",
            "event_analysis",
            "other_information",
            "event_short_name",
            "event_origin",
            "peak_date",
            "peak_dt",
            "credit_tag",
        ]

        # I am ending with a WHERE TRUE SO I can just add sql filters always without adding the check
        if self.request.options.all_events:
            sql = f"""SELECT * FROM (
                SELECT {", ".join(event_columns)} FROM {self.table}
                UNION


                SELECT {", ".join(event_columns)} FROM (SELECT 
                ROW_NUMBER() OVER () + 1000000 AS id,
                expected_dt::date AS start_date,
                expected_dt AS start_dt,
                published_dt::date AS end_date,
                published_dt AS end_dt,
                instrument as event_name,
                'eco_releases' AS event_category, 
                NULL AS event_subcategory,
                (description || ' ' || dtime|| ': ' || COALESCE(last_release::text, '')) AS description,
                NULL AS event_analysis,
                NULL::jsonb AS other_information,
                NULL AS event_short_name,
                country AS event_origin,
                NULL::date AS peak_date,
                NULL::timestamptz AS peak_dt,
                NULL AS credit_tag
                FROM (
                    SELECT instrument, dtime, description, last_release, COALESCE(expected_dt, published_dt) AS expected_dt,  COALESCE(published_dt, expected_dt) AS published_dt, country
                FROM primarydata.eikon_ecoreleases 
                NATURAL JOIN info.ecoreleases
                WHERE expected_dt is NOT NULL or published_dt IS NOT NULL
                ORDER BY dtime ASC, instrument ASC) AS sq) AS eco_releases

                UNION 

                SELECT {", ".join(event_columns)} FROM (
                SELECT 
                ROW_NUMBER() OVER () + 2000000 AS id,
                dtime0::date AS start_date,
                dtime0 AS start_dt,
                dtime1::date AS end_date,
                dtime1 AS end_dt,
                'fed_purchases' as event_name,
                'purchases' AS event_category, 
                NULL AS event_subcategory,
                event_name AS description,
                NULL AS event_analysis,
                NULL::jsonb AS other_information,
                NULL AS event_short_name,
                NULL AS event_origin,
                NULL::date AS peak_date,
                NULL::timestamptz AS peak_dt,
                NULL AS credit_tag
                FROM primarydata.fed_purchases
                ORDER BY dtime0 ASC
                ) AS fed_purchases
                
                UNION 

                SELECT {", ".join(event_columns)} FROM (
                SELECT 
                ROW_NUMBER() OVER () + 3000000 AS id,
                auction_date AS start_date,
                closing_time_competitive AS start_dt,
                auction_date AS end_date,
                closing_time_competitive AS end_dt,
                'usa_auctions' as event_name,
                'auctions' AS event_category, 
                NULL AS event_subcategory,
                'USA Auction ' || security_type || ' ' || security_term_week_year || ', ' || security_term_day_month AS description,
                NULL AS event_analysis,
                NULL::jsonb AS other_information,
                NULL AS event_short_name,
                NULL AS event_origin,
                NULL::date AS peak_date,
                NULL::timestamptz AS peak_dt,
                NULL AS credit_tag
                FROM primarydata.usa_auctions
                ORDER BY auction_date ASC
                ) AS usa_auctions

                UNION 

                SELECT {", ".join(event_columns)} FROM (
                SELECT 
                ROW_NUMBER() OVER () + 4000000 AS id,
                auction_date AS start_date,
                NULL::timestamptz AS start_dt,
                auction_date AS end_date,
                NULL::timestamptz AS end_dt,
                'ger_auctions' as event_name,
                'auctions' AS event_category, 
                NULL AS event_subcategory,
                'Germany Auction ' || bond_type || ' ' || maturity_segment  AS description,
                NULL AS event_analysis,
                NULL::jsonb AS other_information,
                NULL AS event_short_name,
                NULL AS event_origin,
                NULL::date AS peak_date,
                NULL::timestamptz AS peak_dt,
                NULL AS credit_tag
                FROM primarydata.ger_auctions
				ORDER BY auction_date ASC
                ) AS ger_auctions
                
                ) AS all_events
                WHERE TRUE"""
        else:
            sql = f"""SELECT * FROM {self.table}
                        WHERE TRUE"""

        sql_filters = []
        if self.request.filters:
            for k, v in self.request.filters.items():
                if k in event_columns:
                    if isinstance(v, dict):
                        if all([boundary in v for boundary in {"min", "max"}]):
                            sql_filters.append(
                                f"{k} BETWEEN {v['min']} AND {v['max']} "
                            )
                    else:
                        sql_filters.append(
                            f"{k} ILIKE ANY(ARRAY {[vv.replace('*', '%') for vv in v]}) "
                        )
                else:
                    sql_filters.append(
                        f"other_information->>'{k.lower().strip().replace(' ', '_')}' ILIKE ANY(ARRAY {[vv.replace('*', '%') for vv in v]}) "
                    )

        if self.request.from_date:
            sql_filters.append(f"start_date >= '{self.request.from_date}' ")
        if self.request.to_date:
            sql_filters.append(
                f"end_date <= '{self.request.to_date.date().isoformat()}' "
            )

        if sql_filters:
            sql += " AND " + "AND ".join(sql_filters)

        sql += " ORDER BY start_date ASC"
        # if self.request.filters:
        #     for key, value in self.request.filters.items():
        #         sql += _filter_to_sql(key, value)

        return sql


@alru_cache(ttl=10)  # 30 minutes
async def get_drivers(db: DatabaseService) -> list[Driver]:
    """Return the list of drivers from the database."""
    return await db.get_all_assets(
        table="info.drivers_view",
    )


async def get_driver(ticker: str, db: DatabaseService) -> dict:
    """Return the driver dictionary with the given ticker from the list of drivers
    stored in the database"""
    drivers = await get_drivers(db)
    for driver in drivers:
        if driver["driver"] == ticker:
            return driver
    return None


@alru_cache(ttl=10)  # 30 minutes
async def ticker_exists(ticker, table, db):
    sql = f"""
        SELECT EXISTS (
            SELECT 1
            FROM {table}
            where instrument = '{ticker}'
        );
    """
    return await db.conn.fetchval(sql)


async def get_default_table(request, db):
    if await get_driver(request.ticker, db):
        table = CUSTOM_INDEXES_TABLE.format(dtype=request.dtype)
    else:
        try:
            table = TABLES[request.dtype]
        except KeyError:
            raise DataTypeNotFound(
                f"Data type '{request.dtype}' not found.\n"
                "Available data types: "
                f"{', '.join(TABLES.keys())}"
            )

    return table


class BuilderFactory:
    """Factory that returns a builder based on the request type content"""

    @staticmethod
    async def get_builder(
        request: LoadRequest, db: DatabaseService, table: str = None
    ) -> Builder:
        """Return the builder that matches the request type.

        Request types:
            - Generic: EDG1NR, FFG2NR, etc.
            - Audit trail: tt_audittrail dtype
            - Driver: Drivers from info.drivers table
            - Standard: All other tickers

        Parameters
        ----------
        request : LoadRequest
            Request object with the parameters of the request.
        db : DatabaseService
            Database service to use for querying the database. Used to check if a
            ticker is a driver.
        table: str, optional
            Database table to use as source table. This is meant when data is loaded
            from a staging table (e.g. custom_indexes Airflow DAG). If not provided
            the default table based on the request type is used. Default is None.

        Returns
        -------
        Builder
            Builder that matches the request type.
        """
        if table is None:
            table = await get_default_table(request, db)

        # if request.freq is not None:  # todo: wrong approach, freq should be
        #     # handle in each builder
        #     return AggBuilder(request, db, table=table)

        if request.dtype == "tt_audittrail":
            return AuditTrailBuilder(request, db, table=table)
        elif request.dtype == "events":
            return EventBuilder(request, db, table=table)
        elif request.dtype in ["dlveod", "dlvintra"]:
            match = re.match(PRODTYPE_REGEX["GNR"], request.ticker)
            if match:
                product = match.group("product")
                generics = [int(x) for x in match.group("generic").split("_")]
                return DLVBuilder(
                    request, db, table=table, product=product, generics=generics
                )
            else:
                return DLVBuilder(request, db, table=table)
        elif request.dtype in ["spreadeod", "spreadintra"]:
            return SpreadBuilder(request, db, table=table)
        elif request.dtype in ["strategieseod", "strategiesintra"]:
            return StrategyBuilder(request, db, table=table)
        elif request.dtype == "ecoreleases":
            return EcoReleaseBuilder(request, db, table=table)
        elif request.ticker is None:  # ex: scrapers data
            return StandardBuilder(request, db=db, table=table)
        else:
            driver = await get_driver(request.ticker, db)
            if driver:
                return DriverBuilder(request, driver=driver, db=db, table=table)
            else:
                return StandardBuilder(request, db=db, table=table)
