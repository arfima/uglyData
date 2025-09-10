import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from math import ceil
from typing import Any
from contextlib import asynccontextmanager, contextmanager
import pandas as pd

from psycopg.errors import UndefinedTable
import logging

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import psycopg_pool

from ..log import get_logger
from . import _select

LOG = get_logger(__name__)


class FetchFormat(str, Enum):
    record = "record"
    json = "json"
    dataframe = "dataframe"


class AbstractDB(ABC):
    # _instance = None

    # def __new__(cls, *args, **kwargs):
    #     if cls._instance is None:
    #         cls._instance = super().__new__(cls)
    #     return cls._instance
    def __init__(self, debug: bool = False) -> None:
        self._conn = None

        if debug:
            LOG.setLevel(logging.DEBUG)
        else:
            LOG.setLevel(logging.INFO)

    @property
    def conn(self) -> psycopg.AsyncConnection | psycopg.Connection:
        if self._conn is None:
            raise psycopg.errors.InterfaceError("No connection to database")
        return self._conn

    @abstractmethod
    def connect(self):
        pass

    def _base_copy_to_table(
        self,
        table_name: str,
        schema: str = None,
        df: pd.DataFrame = None,
        csv: str = None,
        columns: list[str] = None,
        delimiter="\t",
    ):
        if df is None and csv is None:
            raise ValueError("Either df or csv must be provided")

        if schema:
            table_name = f"{schema}.{table_name}"

        if df is not None:
            csv = df.to_csv(
                index=False, header=False, sep=delimiter, quotechar="🖯"
            ).replace("🖯", "$🖯$")
            columns = df.columns.tolist()
            rows = len(df)
        else:
            rows = len(csv.split("\n")) - 1

        if columns:
            columns_str = f"({', '.join(columns)})"
        else:
            columns_str = ""

        query = f"""COPY {table_name}{columns_str} FROM STDIN  
                        WITH 
                            NULL AS '' 
                            DELIMITER E'{delimiter}'
                """
        return csv, query, rows

    def _base_insert_from_table(self, source, target, on_conflict, columns, pkeys):
        if on_conflict not in ["ignore", "update"]:
            raise ValueError("on_conflict must be either 'ignore' or 'update'")

        if "." in source:
            source_schema, source = source.split(".")
        if "." in target:
            target_schema, target = target.split(".")

        if on_conflict == "update":
            upd_values = sql.SQL(", ").join(
                sql.Composed(
                    [
                        sql.Identifier(k),
                        sql.SQL(" = "),
                        sql.SQL("EXCLUDED.{}").format(sql.Identifier(k)),
                    ]
                )
                for k in columns
                if k not in pkeys
            )
            pkeys_values = sql.SQL(" AND ").join(
                sql.Composed(
                    [
                        sql.Identifier(target_schema, target, k),
                        sql.SQL(" = "),
                        sql.SQL("EXCLUDED.{}").format(sql.Identifier(k)),
                    ]
                )
                for k in pkeys
            )
            conflict_res = sql.SQL("DO UPDATE SET {} WHERE {}").format(
                upd_values, pkeys_values
            )
        else:
            conflict_res = sql.SQL("DO NOTHING")

        query = sql.SQL(
            """
            INSERT INTO {target} SELECT * FROM {source} 
            ON CONFLICT ({pkeys}) {conflict_res}
        """
        ).format(
            target=sql.Identifier(target_schema, target),
            source=sql.Identifier(source_schema, source),
            pkeys=sql.SQL(", ").join(sql.Identifier(k) for k in pkeys),
            conflict_res=conflict_res,
        )
        return query


class DB(AbstractDB):
    def connect(self, conninfo: str = None, service: str = None, **kwargs):
        if conninfo is None and service is None:
            raise ValueError("Either 'conninfo' or 'service' must be provided")
        if service:
            conninfo = f"service={service}"

        self._conn = psycopg.connect(conninfo=conninfo, autocommit=True, **kwargs)
        LOG.debug(f"Connected to database {service or conninfo}")
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self.conn.close()
        LOG.debug("Closed connection to database")

    @contextmanager
    def cursor(self, *args, **kwargs) -> psycopg.Cursor:
        with self.conn.cursor(*args, **kwargs) as cursor:
            yield cursor

    def execute(self, query: str, params=None, *args, **kwargs):
        with self.cursor(*args, **kwargs) as cursor:
            cursor.execute(query, params=params)

    def fetch(self, query: str, params=None, output: FetchFormat = "record"):
        """Fetch data from the database.

        Parameters:
        -----------
        query: str
            SQL query to execute.
        output: FetchFormat
            Format of the output. Can be "record", "json" or "dataframe".

        Returns:
        --------
        list[tuple] or list[dict] or pd.DataFrame depending on the output format.
        """
        output = FetchFormat(output)
        with self.cursor() as cursor:
            cursor.execute(query, params=params)
            data = cursor.fetchall()
            if output == "record":
                return data
            elif output == "json":
                columns = [c.name for c in cursor.description]
                return [dict(zip(columns, row)) for row in data]
            elif output == "dataframe":
                return pd.DataFrame(data, columns=[c.name for c in cursor.description])

    def get_pkeys(self, table_name: str, schema):
        query = f"""
                SELECT a.attname
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                    AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = '{schema}.{table_name}'::regclass
                AND    i.indisprimary;
            """
        try:
            pkeys = self.fetch(query)
        except psycopg.errors.UndefinedTable:
            raise UndefinedTable(f"Table '{table_name}' not found in schema '{schema}'")

        if not pkeys:
            return None

        return [p[0] for p in pkeys]

    def get_columns(self, table_name: str, schema):
        query = f"""
                SELECT column_name, ordinal_position 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND table_schema = '{schema}'
                ORDER BY ordinal_position;
            """

        columns = self.fetch(query)
        if not columns:
            raise UndefinedTable(f"Table '{table_name}' not found in schema '{schema}'")

        return [c[0] for c in columns]

    def copy_to_table(
        self,
        table_name: str,
        schema: str = None,
        df: pd.DataFrame = None,
        csv: str = None,
        columns: list[str] = None,
        delimiter="\t",
    ):
        csv, query, rows = self._base_copy_to_table(
            table_name, schema, df, csv, columns, delimiter
        )

        with self.cursor() as cursor:
            with cursor.copy(query) as copy:
                copy.write(csv)
        return rows

    def insert_from_table(
        self,
        source: str,
        target: str,
        on_conflict: str = "ignore",
    ):
        if "." in target:
            schema, table_name = target.split(".")
        pkeys = self.get_pkeys(table_name, schema)
        columns = self.get_columns(table_name, schema)
        query = self._base_insert_from_table(
            source, target, on_conflict, columns, pkeys
        )
        with self.cursor() as cursor:
            cursor.execute(query)


class AsyncDB(AbstractDB):
    def __enter__(self):
        raise RuntimeError("Use 'async with' instead of 'with'")

    def __exit__(self):
        pass

    async def __aenter__(self):
        # await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self, conninfo: str = None, service: str = None, **kwargs):
        if conninfo is None and service is None:
            raise ValueError("Either 'conninfo' or 'service' must be provided")
        if service:
            conninfo = f"service={service}"

        self._conn = await psycopg.AsyncConnection.connect(
            conninfo=conninfo, autocommit=True, **kwargs
        )
        LOG.debug(f"Connected to database {service or conninfo}")
        return self

    async def close(self):
        await self.conn.close()
        LOG.debug("Closed connection to database")

    @asynccontextmanager
    async def cursor(self, *args, **kwargs) -> psycopg.AsyncCursor:
        async with self.conn.cursor() as cursor:
            yield cursor

    async def execute(self, query: str, params=None, notice=False, *args, **kwargs):
        notice_msg = []
        if notice and isinstance(self.conn, psycopg.AsyncConnection):

            def on_notice(notice):
                nonlocal notice_msg
                notice_msg.append(notice.message_primary)

            self.conn.add_notice_handler(on_notice)

        async with self.cursor(*args, **kwargs) as cursor:
            await cursor.execute(query, params=params)

        return notice_msg

    async def fetch(
        self, query: str, params=None, output: FetchFormat = "record"
    ) -> list[tuple] or list[dict] or pd.DataFrame:
        """Fetch data from the database.

        Parameters:
        -----------
        query: str
            SQL query to execute.
        output: FetchFormat
            Format of the output. Can be "record", "json" or "dataframe".

        Returns:
        --------
        list[tuple] or list[dict] or pd.DataFrame depending on the output format.
        """
        output = FetchFormat(output)
        async with self.cursor() as cursor:
            await cursor.execute(query, params=params)
            data = await cursor.fetchall()
            if output == "record":
                # return await self.conn.fetch(query, *args)
                return data
            elif output == "json":
                columns = [c.name for c in cursor.description]
                return [dict(zip(columns, row)) for row in data]
            elif output == "dataframe":
                return pd.DataFrame(data, columns=[c.name for c in cursor.description])

    async def fetchval(self, query: str, params=None) -> Any:
        async with self.cursor() as cursor:
            await cursor.execute(query, params=params)
            return (await cursor.fetchone())[0]

    async def select(
        self,
        table: str,
        filters: dict[str, Any] = None,
        fields: list[str] = None,
        limit: int = None,
        offset: int = None,
        output: FetchFormat = "record",
        sorting: list[str] = None,
        search_query: str = None,
        search_columns: list[str] = None,
        return_just_count: bool = False,
    ) -> list[dict]:
        """Build a SELECT query and execute it.

        Parameters:
            table (str): The table name.
            filters (dict[str, Any], optional): Dictionary of filters. Defaults to None.
            fields (list[str], optional): List of fields to select. Defaults to None.
            limit (int, optional): Maximum number of rows to return. Defaults to None.
            offset (int, optional): Number of rows to skip. Defaults to None.
            output (FetchFormat, optional): Format of the output. Defaults to "record".
            sorting (list[str], optional): List of sorting conditions. Defaults to None.
            search_query (str, optional): Search query string. Defaults to None.
            search_columns (list[str], optional): List of columns to search in. Defaults
            to None. It is required if search_query is specified.

        Returns:
            list[dict]: List of selected records.

        Raises:
            ValueError: If search_columns is not specified for search_query.

        """
        params = []

        if return_just_count:
            query = sql.SQL("SELECT COUNT(*) FROM (")
        else:
            query = sql.SQL("")

        fields = _select.get_fields_expression(fields)

        schema, table = _select.split_table_name(table)
        query += _select.build_select_query(fields, schema, table)
        if filters or search_query:
            query += sql.SQL(" WHERE ")
        query, params = _select.add_filters(query, filters, params)
        if filters and search_query:
            query += sql.SQL(" AND (")
        query, params = _select.add_search_condition(
            query, search_query, search_columns, params
        )
        if filters and search_query:
            query += sql.SQL(") ")
        query, params = _select.add_sorting(
            query, params, search_query, search_columns, sorting
        )
        query = _select.add_limit_and_offset(query, limit, offset)
        if return_just_count:
            query += sql.SQL(") as _count;")
        return await self.fetch(query, params=params, output=output)

    async def paginate(self, query: str, page_size: int, output: str = "record"):
        """
        Paginate a query and return a generator of pages.

        Parameters
        ----------
        query : str
            The query to paginate
        page_size : int
            The number of rows per page
        output : str, optional
            The format of the output, by default "record". Can be "record", "json"
            or "dataframe"

        Yields
        -------
        list[dict] or pd.DataFrame or asyncpg.Record
            A page of the query
        """
        total_rows = await self.fetchval(
            f"SELECT COUNT(*) FROM ({query}) as _paginate;"
        )
        if total_rows == 0:
            yield pd.DataFrame() if output == FetchFormat.dataframe else []
        else:
            n_pages = ceil(total_rows / page_size)
            if n_pages == 1:  # not needed to paginate
                yield await self.fetch(query, output=output)
            else:
                for page in range(n_pages):
                    offset = page * page_size
                    yield await self.fetch(
                        f"{query} LIMIT {page_size} OFFSET {offset}", output=output
                    )

    async def cursor_paginating(
        self,
        query: str,
        page_size: int,
        output: str = "record",
        return_count: bool = False,
    ):
        """
        Paginate a query and return a generator of cursors.

        Parameters
        ----------
        query : str
            The query to paginate
        output : str, optional
            The format of the output, by default "record". Can be "record", "json"
            or "dataframe"

        Yields
        -------
        list[dict] or pd.DataFrame or asyncpg.Record
            A page of the query
        """

        async with self.cursor() as cursor:
            await cursor.execute(query)
            if return_count:
                yield cursor.rowcount
            rows = await cursor.fetchmany(page_size)
            yield pd.DataFrame(rows, columns=[c.name for c in cursor.description])
            while len(rows) == page_size:
                rows = await cursor.fetchmany(page_size)
                yield pd.DataFrame(rows, columns=[c.name for c in cursor.description])

    async def get_columns(self, table_name: str, schema: str):
        records = await self.fetch(
            f"""
                SELECT column_name, ordinal_position 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND table_schema = '{schema}'
                ORDER BY ordinal_position;
            """
        )
        if not records:
            raise UndefinedTable(f"Table '{table_name}' not found in schema '{schema}'")
        # postgresql does not guarantee the order of the columns
        # sorted_cols = sorted(records, key=lambda x: x["ordinal_position"])
        return [r[0] for r in records]

    async def is_hypertable(self, table_name: str, schema: str):
        records = await self.fetch(
            f"""
                SELECT 1
                FROM timescaledb_information.hypertables
                WHERE hypertable_name = '{table_name}'
                AND hypertable_schema = '{schema}'
                LIMIT 1;
            """
        )
        if records:
            return True
        else:
            return False

    async def get_pkeys(self, table_name: str, schema: str):
        records = await self.fetch(
            f"""
                SELECT a.attname
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                    AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = '{schema}.{table_name}'::regclass
                AND    i.indisprimary;
            """
        )
        if not records:
            query = f"""SELECT EXISTS 
                (SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{table_name}' and table_schema = '{schema}');"""
            exists = await self.fetchval(query)
            if not exists:
                raise UndefinedTable(
                    f"Table '{table_name}' not found in schema '{schema}'"
                )
            else:
                return None
        return [r[0] for r in records]

    async def copy_to_table(
        self,
        table_name: str,
        schema: str = None,
        df: pd.DataFrame = None,
        csv: str = None,
        columns: list[str] = None,
        delimiter="\t",
    ):
        csv, query, rows = self._base_copy_to_table(
            table_name, schema, df, csv, columns, delimiter
        )

        async with self.cursor() as cursor:
            async with cursor.copy(query) as copy:
                await copy.write(csv)
        return rows

    async def insert(
        self,
        table: str,
        values: list[tuple],
        columns: list[str] = None,
        discard_duplicates: bool = False,
    ):
        if columns:
            columns_str = f"({', '.join(columns)})"
        else:
            columns_str = ""

        values_str = "(" + ", ".join(["%s" for _ in values]) + ")"
        query = f"INSERT INTO {table}{columns_str} VALUES {values_str}"
        if discard_duplicates:
            query += " ON CONFLICT DO NOTHING "

        query += "RETURNING *"
        async with self.cursor() as cursor:
            await cursor.execute(query, values)
            inserted = await cursor.fetchall()
            columns = [c.name for c in cursor.description]
        results = [dict(zip(columns, row)) for row in inserted]
        return results if len(results) > 1 else results[0]

    async def insert_from_table(
        self, source: str, target: str, on_conflict: str = "ignore"
    ):
        if "." in target:
            schema, table_name = target.split(".")

        pkeys = await self.get_pkeys(table_name, schema=schema)
        columns = await self.get_columns(table_name, schema=schema)
        query = self._base_insert_from_table(
            source, target, on_conflict, columns, pkeys
        )
        async with self.cursor() as cursor:
            await cursor.execute(query)

    async def insert_many(
        self,
        table: str,
        values: list[tuple],
        columns: list[str] = None,
        discard_duplicates: bool = False,
    ):
        if "." in table:
            schema, table = table.split(".")

        if columns:
            columns_str = f"({', '.join(columns)})"
        else:
            columns_str = ""
        async with self.cursor() as cursor:
            query = sql.SQL("INSERT INTO {}{} VALUES {}{} RETURNING *").format(
                sql.Identifier(schema, table),
                sql.SQL(columns_str),
                sql.SQL(", ").join(
                    sql.Composed(
                        [
                            sql.SQL("("),
                            sql.SQL(", ").join(sql.Placeholder() * len(row)),
                            sql.SQL(")"),
                        ]
                    )
                    for row in values
                ),
                (
                    sql.SQL(" ON CONFLICT DO NOTHING")
                    if discard_duplicates
                    else sql.SQL("")
                ),
            )
            flattened_values = [value for row in values for value in row]
            await cursor.execute(query, flattened_values)
            inserted = await cursor.fetchall()
            columns = [c.name for c in cursor.description]
        return [dict(zip(columns, row)) for row in inserted]

    async def update(self, table: str, values: dict[str, Any], pkeys: str or list[str]):
        """Update data in the database.

        Parameters
        ----------
        values : dict
            Data to be updated as dict. Keys must be the same as the column names in
            the database.
        pkeys : str or list[str]
            The fields from the values dict that are primary keys in the database.
        """
        if "." in table:
            schema, table = table.split(".")

        if isinstance(pkeys, str):
            pkeys = [pkeys]

        pkeys_conditions1 = sql.SQL(" AND ").join(
            sql.Composed(
                [
                    sql.Identifier("old_t", k),
                    sql.SQL(" = "),
                    sql.Identifier("new_t", k),
                ]
            )
            for k in pkeys
        )

        pkeys_conditions = sql.SQL(" AND ").join(
            sql.Composed(
                [
                    sql.Identifier("new_t", k),
                    sql.SQL(" = "),
                    sql.Placeholder(k),
                ]
            )
            for k in pkeys
        )

        upd_values = sql.SQL(", ").join(
            sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder(k)])
            for k in values.keys()
        )

        query = sql.SQL(
            "UPDATE {table} new_t SET {values} FROM {table} old_t WHERE "
            " {pkeys_conditions1} AND {pkeys_conditions} "
            "RETURNING old_t.*"
        ).format(
            table=sql.Identifier(schema, table),
            values=upd_values,
            pkeys_conditions1=pkeys_conditions1,
            pkeys_conditions=pkeys_conditions,
        )

        async with self.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query, values)
            return await cursor.fetchall()

    async def delete_and_insert(
        self, table: str, values: list[dict[str, Any]], pkeys: str or list[str]
    ):
        if "." in table:
            schema, table = table.split(".")

        async with self.cursor(row_factory=dict_row) as cursor:
            query = sql.SQL(
                """
                WITH deletions AS (DELETE FROM {} WHERE {} RETURNING *),
                inserted as (INSERT INTO {} VALUES {})
                select * from deletions


            """
            ).format(
                sql.Identifier(schema, table),
                sql.SQL(" AND ").join(
                    sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder()])
                    for k in pkeys
                ),
                sql.Identifier(schema, table),
                sql.SQL(", ").join(
                    sql.Composed(
                        [
                            sql.SQL("("),
                            sql.SQL(", ").join(sql.Placeholder() * len(row)),
                            sql.SQL(")"),
                        ]
                    )
                    for row in values
                ),
            )
            pkeys_values = [values[0][pkey] for pkey in pkeys]
            flattened_values = pkeys_values + [
                value for row in values for key, value in row.items()
            ]
            await cursor.execute(query, flattened_values)
            return await cursor.fetchall()

    async def upsert(
        self, table: str, values: list[dict[str, Any]], pkeys: str or list[str]
    ):
        if "." in table:
            schema, table = table.split(".")

        async with self.cursor() as cursor:
            query = sql.SQL(
                "INSERT INTO {} VALUES {} ON CONFLICT {} DO UPDATE SET {}"
            ).format(
                sql.Identifier(schema, table),
                sql.SQL(", ").join(
                    sql.Composed(
                        [
                            sql.SQL("("),
                            sql.SQL(", ").join(sql.Placeholder() * len(row)),
                            sql.SQL(")"),
                        ]
                    )
                    for row in values
                ),
                sql.SQL("({})").format(
                    sql.SQL(", ").join(sql.Identifier(k) for k in pkeys)
                ),
                sql.SQL(", ").join(
                    sql.Composed(
                        [
                            sql.Identifier(k),
                            sql.SQL(" = "),
                            sql.SQL("EXCLUDED.{}").format(sql.Identifier(k)),
                        ]
                    )
                    for k in values[0].keys()
                ),
            )
            flattened_values = [value for row in values for value in row.values()]
            await cursor.execute(query, flattened_values)

    async def delete(
        self, table: str, values: list[dict[str, Any]], pkeys: str or list[str]
    ):
        if "." in table:
            schema, table = table.split(".")

        async with self.cursor(row_factory=dict_row) as cursor:
            query = sql.SQL("DELETE FROM {} WHERE {} RETURNING *").format(
                sql.Identifier(schema, table),
                sql.SQL(" AND ").join(
                    sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder()])
                    for k in pkeys
                ),
            )
            pkeys_values = [values[pkey] for pkey in pkeys]
            await cursor.execute(query, pkeys_values)
            return await cursor.fetchall()


class AsyncDBPool(AsyncDB):
    def __init__(
        self,
        min_size: int = 1,
        max_size: int = 10,
        num_workers: int = 3,
        debug: bool = False,
        default_autocommit: bool = False,
    ):
        super().__init__()
        self.min_size = min_size
        self.max_size = max_size
        self.num_workers = num_workers
        self._conn = None
        self._transactions = {}
        self.default_autocommit = default_autocommit
        if debug:
            LOG.setLevel(logging.DEBUG)
        else:
            LOG.setLevel(logging.INFO)

    @asynccontextmanager
    async def cursor(self, autocommit=None, *args, **kwargs):
        if autocommit is None:
            autocommit = self.default_autocommit
        task = asyncio.current_task()
        if task in self._transactions and self._transactions[task] is not None:
            conn = self._transactions[task]
            async with conn.cursor(*args, **kwargs) as cursor:
                yield cursor
        else:
            await self._conn.check()
            async with self._conn.connection() as conn:
                try:
                    await conn.set_autocommit(autocommit)
                    async with conn.cursor(*args, **kwargs) as cursor:
                        yield cursor
                    if autocommit != self.default_autocommit:
                        await conn.set_autocommit(self.default_autocommit)
                except Exception as excep:
                    try:
                        await cursor.execute(
                            """SELECT client_port FROM pg_stat_activity WHERE pid = pg_backend_pid()"""
                        )  # await cursor.execute("SELECT pg_backend_pid()")
                        pid = await cursor.fetchone()
                    except Exception as e_pid:
                        pid = ["unknown"]
                        print(
                            f"ERROR in AsyncDBPool.cursor: failed to recover pid for {asyncio.current_task().get_name()}",
                            e_pid,
                        )
                    print(
                        f"ERROR in AsyncDBPool.cursor failed in 'with connection as conn' block pid:{pid[0]}: Failed before commit/close",
                        asyncio.current_task(),
                        excep,
                    )
                    raise excep
                finally:
                    await conn.commit()  # added cause not sure if xwith this pool it does the auto commit close
                    await conn.close()  # added cause not sure if xwith this pool it does the auto commit close

    @asynccontextmanager
    async def transaction(self):
        await self._conn.check()
        async with self._conn.connection() as conn:
            self._transactions[asyncio.current_task()] = conn
            yield conn
            # async with conn.cursor() as cursor:
            # self._transactions[asyncio.current_task()] = cursor
            # yield cursor
        self._transactions[asyncio.current_task()] = None

    @asynccontextmanager
    async def connection(self):
        await self._conn.check()
        async with self._conn.connection() as conn:
            yield conn

    async def connect(self, conninfo: str = None, service: str = None, **kwargs):
        if conninfo is None and service is None:
            raise ValueError("Either 'conninfo' or 'service' must be provided")
        if service:
            conninfo = f"service={service}"

        self._conn = psycopg_pool.AsyncConnectionPool(
            conninfo=conninfo,
            min_size=self.min_size,
            max_size=self.max_size,
            num_workers=self.num_workers,
            open=False,
            **kwargs,
        )
        await self._conn.open()

        LOG.debug(
            "Pool connected to database %s with %d-%d connections.",
            *[self._conn.conninfo, self.min_size, self.max_size],
        )
        return self
