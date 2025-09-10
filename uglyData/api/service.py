from datetime import datetime

import logging
import pandas as pd

from uglyData.ingestor import TSLib

from ..db.elastic import ESClient
from ..db.postgres import AsyncDB, AsyncDBPool
from .exceptions import (
    AssetNotFound,
    AssetAlreadyExists,
    FieldNotValid,
    ForeignKeyViolationError,
)

from uglyData.api.models import LoadRequest
from .builders import BuilderFactory
from psycopg.errors import UniqueViolation, ForeignKeyViolation

LOG = logging.getLogger()

ASSETS_SCHEMA = "info"


class DatabaseService:
    """Business logic for the database"""

    BARS_ROW_LIMIT = 1440
    TICKS_ROW_LIMIT = 10000

    def __init__(self, db: AsyncDB):
        self.conn: AsyncDB = db

    async def connect(self, *args, **kwargs):
        await self.conn.connect(*args, **kwargs)

    async def close(self):
        await self.conn.close()

    async def get_asset(self, table: str, values: dict):
        """Get a single asset by name"""
        results = await self.conn.select(
            table=table,
            filters=values,
            output="json",
        )
        if len(results) == 0:
            raise AssetNotFound("Item not found.")
        return results[0]

    async def get_audittrail_ftp_files(self):
        return await self.get_all_assets(
            table="info.audittrail_ftp_files",
        )

    async def get_categories(self, table: str, category: str):
        sql = f"""
            SELECT DISTINCT {category} FROM {table}
            ORDER BY 1
        """
        return await self.conn.fetch(sql)

    async def get_enum(self, my_enum: str):
        """Return the values of an enum as records."""
        sql = f"""
            SELECT unnest(enum_range(NULL::{my_enum}))
        """
        return await self.conn.fetch(sql)

    async def get_all_assets(
        self,
        table: str,
        filters: dict = None,
        limit: int = None,
        offset: int = None,
        search_query: str = None,
        sorting: list[str] = None,
        return_just_count: bool = False,
        *args,
        **kwargs,
    ):
        """Get all assets from a table"""
        return await self.conn.select(
            table=table,
            limit=limit,
            filters=filters,
            offset=offset,
            output="json",
            search_query=search_query,
            sorting=sorting,
            return_just_count=return_just_count,
            *args,
            **kwargs,
        )

    async def add_asset(
        self,
        table: str,
        asset: dict | list[dict],
        discard_duplicates: bool = False,
    ):
        """Add an asset or collection of assets to the database"""
        # asset model to list of tuples
        try:
            if isinstance(asset, dict):
                values = tuple(asset.values())
                columns = list(asset.keys())
                return await self.conn.insert(
                    table=table,
                    values=values,
                    columns=columns,
                    discard_duplicates=discard_duplicates,
                )
            elif isinstance(asset, list):
                values = [tuple(a.values()) for a in asset]
                columns = list(asset[0].keys())
                return await self.conn.insert_many(
                    table=table,
                    values=values,
                    columns=columns,
                    discard_duplicates=discard_duplicates,
                )
            else:
                raise TypeError("Asset must be a dict or list of dicts")
        except UniqueViolation:
            raise AssetAlreadyExists(f"Item already exists: {asset}")
        except ForeignKeyViolation as e:
            raise FieldNotValid(e)

    async def update_asset(
        self, table: str, asset: dict | list[dict], pkeys: list[str]
    ) -> dict | list[dict]:
        """Update an asset in the database

        Parameters
        ----------
        asset : dict
            Asset to be updated.
        pkeys : str or list[str]
            The fields from the Pydantic model that are primary keys in the database.
        """
        try:
            if isinstance(asset, dict):
                affected_rows = await self.conn.update(
                    table=table, values=asset, pkeys=pkeys
                )
                if len(affected_rows) == 0:
                    raise AssetNotFound("Item not found")
                else:
                    AssetModel = asset.__class__
                    return AssetModel(**affected_rows[0])

            elif isinstance(asset, list):
                values = [a for a in asset]
                affected_rows = await self.conn.delete_and_insert(
                    table=table, values=values, pkeys=pkeys
                )

                if len(affected_rows) == 0:
                    return None
                else:
                    AssetModel = asset[0].__class__
                    return [AssetModel(**row) for row in affected_rows]

            else:
                raise TypeError("Asset must be a dict or list of dicts")
        except ForeignKeyViolation as e:
            raise FieldNotValid(e)

    # async def upsert_asset(
    #     self, table: str, asset: BaseModel or list[BaseModel], pkeys: list[str]
    # ) -> BaseModel or list[BaseModel]:
    #     """Upsert an asset in the database

    #     Parameters
    #     ----------
    #     asset : BaseModel
    #         Asset to be updated.
    #     pkeys : str or list[str]
    #         The fields from the Pydantic model that are primary keys in the database.
    #     """
    #     try:
    #         if isinstance(asset, BaseModel):
    #             values = asset.model_dump()
    #             affected_rows = await self.conn.update(
    #                 table=table, values=values, pkeys=pkeys
    #             )
    #             if len(affected_rows) == 0:
    #                 raise AssetNotFound("Item not found")
    #             else:
    #                 AssetModel = asset.__class__
    #                 return AssetModel(**affected_rows[0])

    #         elif isinstance(asset, list):
    #             values = [a.model_dump() for a in asset]
    #             affected_rows = await self.conn.delete_and_insert(
    #                 table=table, values=values, pkeys=pkeys
    #             )

    #             if len(affected_rows) == 0:
    #                 return None
    #             else:
    #                 AssetModel = asset[0].__class__
    #                 return [AssetModel(**row) for row in affected_rows]

    #         else:
    #             raise TypeError("Asset must be a BaseModel or list of BaseModels")
    #     except ForeignKeyViolation as e:
    #         raise FieldNotValid(e)

    async def delete_asset(
        self, table: str, asset: dict | list[dict], pkeys: list[str]
    ):
        """Delete an asset from the database

        Parameters
        ----------
        asset : dict
            Asset to be deleted.
        pkeys : str or list[str]
            The fields from the Pydantic model that are primary keys in the database.
        """
        try:
            if isinstance(asset, dict):
                values = asset
            elif isinstance(asset, list):
                values = asset[0]
            else:
                raise TypeError("Asset must be a dict or list of dicts")

            affected_rows = await self.conn.delete(
                table=table, values=values, pkeys=pkeys
            )
            if len(affected_rows) == 0:
                raise AssetNotFound("Item not found")
            else:
                return affected_rows

        except ForeignKeyViolation as e:
            raise ForeignKeyViolationError(e)

    def _build_market_data_sql(
        self,
        source: str,
        tabtype: str,
        start: str | datetime,
        end: str | datetime,
        fields: list[str] = None,
        freq: str = None,
        limit: int = None,
    ):
        if fields is None:
            fields = "*"
        else:
            fields = ", ".join(fields)

        sql = f"""
            SELECT {fields} FROM {source}_{tabtype}
            WHERE dtime >= '{start}' AND dtime <= '{end}'
        """
        if limit is not None:
            sql += f" LIMIT {limit}"
        return sql

    async def get_market_data_for_instrument(
        self,
        instrument: str,
        tabtype: str,
        source: str,
        start: str | datetime,
        end: str | datetime,
        fields: list[str] = None,
        freq: str = None,
        limit: int = None,
        stream: bool = False,
    ):
        """Get market data for a single instrument"""
        if fields is None:
            fields = "*"
        else:
            fields = ", ".join(fields)

        sql = self._build_market_data_sql(
            source, tabtype, start, end, fields, freq, limit
        )

        if stream:
            return self.conn.fetch_stream(sql)
        else:
            return await self.conn.fetch(query=sql, output="json")

    def _build_sql_load_market_data(
        self, source: str, tabtype: str, requests: list[LoadRequest]
    ):
        """Build SQL query for loading market data for multiple instruments"""
        if not isinstance(requests, list):
            requests = [requests]
        sql = f"""SELECT * FROM {source}_{tabtype} WHERE"""

        for req in requests:
            sql += f""" (instrument = '{req.ticker}' AND dtime >= '{req.from_date}' 
            AND dtime <='{req.to_date}') OR """

        sql = sql[:-3]
        # * don't finish the query with ';', it will break the query with pagination
        return sql

    async def load_market_data(
        self,
        source: str,
        tabtype: str,
        requests: list[LoadRequest],
        stream: bool = False,
    ):
        """Load market data for multiple instruments"""
        sql = self._build_sql_load_market_data(source, tabtype, requests)

        # if not stream:
        return await self.conn.fetch(query=sql, output="json")
        # else:
        #     async for row in self.conn.fetch_stream(sql):
        #         yield row

    async def paginate_market_data(
        self,
        request: LoadRequest,
        page_size: int,
        output: str,
        return_count: bool = False,
    ):
        """
        Paginate market data for a single instrument

        Parameters
        ----------
        source : str
            Source of the data. Used to find the correct table
        tabtype : str
            Type of the table
        request : LoadRequest
            Client request with the parameters to perform the query
        page_size : int
            Size of the page
        output : str
            Output format. Can be 'records', 'json' or 'dataframe'
        freq : str
            a string  representing an interval, for example: "1h", "1d" or an
            timedelta object.
        return_count : bool, optional
            If True, return the count of the query, by default False

        Yields
        -------
        list[asyncpg.Record] | list[dict] | pd.DataFrame
            Data from the database
        """

        builder = await BuilderFactory.get_builder(request, db=self)
        query = await builder.build_sql()

        async for data in self.conn.cursor_paginating(
            query=query,
            page_size=page_size,
            output=output,
            return_count=return_count,
        ):
            yield data

    async def get_count(self, table):
        sql = f"SELECT COUNT(*) FROM {table}"
        return await self.conn.fetchval(sql)

    async def get_range_dates_product(
        self, product: str, product_type: str
    ) -> tuple[datetime, datetime]:
        """Get the range of dates for a product

        Parameters
        ----------
        product : str
            Product name
        product_type : str
            Product type

        Returns
        -------
        tuple[datetime, datetime]
            The minimum and maximum dates for a product
        """
        sql = f"""
            SELECT MIN("start"), MAX("end") FROM primarydata.instruments_info
            where product = '{product}' and product_type = '{product_type}'
        """
        rows = await self.conn.fetch(sql)
        return rows[0]


class ElasticService:
    def __init__(self, db: ESClient) -> None:
        self.db = db

    async def connect(self):
        await self.db.connect()

    async def close(self):
        await self.db.close()

    async def get_outrights_curves(self):
        # TODO: In the future curves will be objects from PSQL Database.
        body = {
            "aggs": {"unique_ids": {"terms": {"field": "data.curve", "size": 10000}}},
            "size": 0,
        }
        result = await self.db.conn.search(body=body)
        curves = [res["key"] for res in result["aggregations"]["unique_ids"]["buckets"]]
        return curves

    async def get_outrights_instruments(self, curve: str = None):
        body = {
            "aggs": {
                "unique_ids": {"terms": {"field": "data.contract", "size": 10000}}
            },
        }

        if curve:
            query = {"term": {"data.curve": curve}}
            body["query"] = query

        result = await self.db.conn.search(index="metrics-ordata-default", body=body)
        instruments = [
            res["key"] for res in result["aggregations"]["unique_ids"]["buckets"]
        ]
        return instruments

    def get_realtime_outrights_data(
        self,
        instruments: str | list[str],
        curve: str | list[str],
        start_at: datetime,
        end_at: datetime,
        size: int = 100,
    ):
        """Get realtime data for a list of instruments and curves"""
        if isinstance(instruments, str):
            instruments = [instruments]
        if isinstance(curve, str):
            curve = [curve]

        body = {
            "size": size,
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"instrument": instruments}},
                        {"terms": {"curve": curve}},
                    ]
                }
            },
        }
        return self.db.search(index="metrics-ordata-default", body=body)

    async def get_last_outrights_data(
        self, contracts: list[str], size: int = 1, fields: list[str] = None
    ):
        body = {
            "query": {"terms": {"data.contract": contracts}},
            "aggs": {
                "last_quote": {
                    "top_hits": {
                        "size": len(contracts) * size,
                        "sort": {"@timestamp": {"order": "desc"}},
                    }
                }
            },
            "size": 0,
        }
        res = await self.db.conn.search(index="metrics-ordata-default", body=body)
        hits = res["aggregations"]["last_quote"]["hits"]
        data = [
            {**{"timestamp": hit["_source"]["@timestamp"]}, **hit["_source"]["data"]}
            for hit in hits["hits"]
        ]
        return self.parse_dataframe(data)

    async def get_count(self, request, **kwargs):
        body = self.query_realtime_data(request)
        # Count don't support sort and size
        body.pop("sort", None)
        body.pop("size", None)
        count = await self.db.conn.count(index="metrics-ordata-default", body=body)
        count = dict(count)
        if count["count"] >= DB.TICKS_ROW_LIMIT:
            count["count"] = DB.TICKS_ROW_LIMIT
        count.pop("_shards", None)
        return count

    def parse_dataframe(self, data):
        if not data:
            return pd.DataFrame()
        data = [
            {**{"timestamp": hit["_source"]["@timestamp"]}, **hit["_source"]["data"]}
            for hit in data
        ]
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.replace(-1, None, inplace=True)
        return df.set_index("timestamp")

    def query_realtime_data(self, request: LoadRequest):
        """Query realtime data for a single instrument"""
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"data.contract": request.ticker}},
                    ]
                }
            },
            "sort": [{"@timestamp": {"order": "asc"}}],
        }
        if request.from_date or request.to_date:
            range_query = {"range": {"@timestamp": {}}}
            ts_params = range_query["range"]["@timestamp"]
            if request.from_date:
                ts_params["gte"] = request.from_date
            if request.to_date:
                ts_params["lte"] = request.to_date

            body["query"]["bool"]["must"].append(range_query)

        if request.from_date is None or request.to_date is None:
            body["size"] = DatabaseService.TICKS_ROW_LIMIT

        return body

    async def paginate_realtime_data(self, request, page_size, output):
        """Paginate realtime data for a single instrument"""
        body = self.query_realtime_data(request)

        async for data in self.db.paginate(body=body, page_size=page_size):
            df = self.parse_dataframe(data)
            yield df


pool = AsyncDBPool(min_size=1, max_size=10)
DB = DatabaseService(db=pool)
# ElasticDB = ElasticService(db=ESClient(dbname="elastic"))
ts_lib = TSLib(db=pool)
