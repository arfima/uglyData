"""
This module contains the TSLib class, which is used to insert data into a
TimescaleDB database.

The TSLib class is used to insert data into a TimescaleDB database. It
provides a simple interface to insert data into a table, taking care of
creating a temporary table, checking for duplicates, and handling conflicts
when inserting data into a hypertable.
"""

import sys
import asyncio
import logging
import pandas as pd
from psycopg.errors import (
    FeatureNotSupported,
    UniqueViolation,
    NoDataFound,
    IndeterminateDatatype,
)
from ..db import AsyncDB
from .timescale import ON_CONFLICT
from . import timescale
from ..log import get_logger

if not sys.platform.startswith("win"):
    import uvloop

    loop = asyncio.get_event_loop()
    if loop.is_running() and not isinstance(loop, uvloop.Loop):
        import nest_asyncio

        nest_asyncio.apply()

LOG = get_logger(__name__)


DEFAULT_SCHEMA = "primarydata"


class TSLib:
    def __init__(
        self,
        db: AsyncDB = None,
        debug: bool = False,
        infer_tz: bool = False,
    ):
        if debug:
            LOG.setLevel(logging.DEBUG)
        else:
            LOG.setLevel(logging.INFO)

        if db is not None:
            self.conn = db
        else:
            self.conn = AsyncDB(debug=debug)

        self.infer_tz = infer_tz
        self.cache = {"is_ht": {}}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self, *args, **kwargs):
        await self.conn.connect(*args, **kwargs)
        return self

    async def close(self):
        await self.conn.close()

    async def _is_hypertable(self, table, schema):
        table_full_name = f"{schema}.{table}"
        if table_full_name not in self.cache["is_ht"]:
            is_ht = await self.conn.is_hypertable(table_name=table, schema=schema)
            self.cache["is_ht"][table_full_name] = is_ht
            LOG.debug(f"Checked if {table_full_name} is a hypertable")
        else:
            is_ht = self.cache["is_ht"][table_full_name]
            LOG.debug(f"Using cached hypertable status for {table_full_name}")

        return is_ht

    async def _get_columns_info(self, table, schema):
        table_full_name = f"{schema}.{table}"
        if table_full_name not in self.cache:
            columns = await self.conn.get_columns(table_name=table, schema=schema)
            pkeys = await self.conn.get_pkeys(table_name=table, schema=schema)
            self.cache[table_full_name] = {"columns": columns, "pkeys": pkeys}
            LOG.debug(f"Requested columns info for {table_full_name}")
        else:
            columns = self.cache[table_full_name]["columns"]
            pkeys = self.cache[table_full_name]["pkeys"]
            LOG.debug(f"Using cached columns info for {table_full_name}")

        return columns, pkeys

    def _check_duplicates(self, df, pkeys, drop_duplicates=True):
        """Check for duplicates in the dataframe according to the primary keys
        of the table"""
        if len(pkeys) > 0:
            # Check for duplicates
            dupes = df.duplicated(subset=pkeys, keep=False)
            if dupes.any():
                if drop_duplicates:
                    df.drop_duplicates(subset=pkeys, keep="last", inplace=True)
                else:
                    raise ValueError(
                        f"Duplicate values found in primary key columns {pkeys}"
                    )
        return df

    def _prepare_dataframe(self, df, columns, pkeys, drop_duplicates):
        """Prepare and validate the dataframe for insertion into the database"""
        if df.empty or len(df.columns) <= 1:
            raise ValueError("Dataframe must be non-empty and have at least 2 columns")
        if pkeys is not None and all(pk in df.columns for pk in pkeys):
            df = self._check_duplicates(df, pkeys, drop_duplicates)
        df = df[[a for a in columns if a in df.columns]]
        return df

    def _check_columns(self, columns: list[str], db_columns: list[str], table: str):
        """Check that the columns to be inserted are in the table"""
        for c in columns:
            if c not in db_columns:
                raise ValueError(
                    f"Column {c} not found in table {table}. ",
                    f"Must be one of {db_columns}",
                )

    async def _insert_data(
        self,
        table: str,
        schema: str,
        df: pd.DataFrame,
        csv: str,
        columns: list[str],
        pkeys: list[str],
        on_conflict: ON_CONFLICT,
        force_nulls: bool,
        recompress_after: bool,
    ):
        """Insert data into the database"""
        try:
            rows = await self.conn.copy_to_table(
                df=df, csv=csv, columns=columns, table_name=table, schema=schema
            )
        except UniqueViolation:
            LOG.debug("Handling unique violation error")
            rows = await self._run_backfilling_procedure(
                table=table,
                schema=schema,
                df=df,
                csv=csv,
                columns=columns,
                pkeys=pkeys,
                on_conflict=on_conflict,
                force_nulls=force_nulls,
                recompress_after=recompress_after,
            )
        except FeatureNotSupported as e:
            if "compressed chunk" in str(e):
                LOG.debug("Handling compressed chunk error")
                rows = await self._run_backfilling_procedure(
                    table=table,
                    schema=schema,
                    df=df,
                    csv=csv,
                    columns=columns,
                    pkeys=pkeys,
                    on_conflict=on_conflict,
                    force_nulls=force_nulls,
                    recompress_after=recompress_after,
                )
            else:
                raise e
        LOG.info(f"Inserted {rows} rows into {schema}.{table}")
        return rows

    async def insert(
        self,
        table: str,
        schema: str = DEFAULT_SCHEMA,
        df: pd.DataFrame = None,
        csv: str = None,
        columns: list[str] = None,
        on_conflict: ON_CONFLICT = ON_CONFLICT.DO_NOTHING,
        force_nulls: bool = False,
        recompress_after: bool = True,
        drop_duplicates: bool = True,
    ):
        """Inserts a dataframe into a table.

        Parameters
        ----------
        table : str
            Name of the table to insert into
        schema : str, optional
            Schema of the table, by default primarydata
        df : pd.DataFrame, optional
            Dataframe to insert. Columns must match the table columns. Mutually
            exclusive with `csv` and `columns` parameters.
        csv : str, optional
            String containing the CSV data to insert. Mutually
            exclusive with `df`
        columns : list, optional
            List of columns of the CSV to insert. If not provided, all columns will be
            inserted. Mutually exclusive with `df` and only valid when `csv` is provided
        on_conflict : ON_CONFLICT, optional
            What to do on conflict, possible values are:
            - 'do_nothing'
            - 'update'
            By default 'do_nothing'
        force_nulls : bool, optional
            If false, null values in the passed dataframe will be ignored when
            updating. If true, null values will be inserted into the database.
            By default False
        recompress_after : bool, optional
            If true, the table chunks will be recompressed after the insert.
            By default True
        drop_duplicates : bool, optional
            If true, duplicate rows will be dropped from the dataframe before
            inserting. By default True

        Returns
        -------
        int
            Number of rows inserted/updated
        """
        if df is None and csv is None:
            raise ValueError("Either df or csv must be provided")

        on_conflict = ON_CONFLICT(on_conflict)

        db_columns, pkeys = await self._get_columns_info(table, schema)

        if on_conflict != ON_CONFLICT.DO_NOTHING and pkeys is None:
            raise ValueError(
                "Table must have primary keys defined to use 'update' on conflict"
            )  # ! Mover esto más arriba

        if df is not None:
            df = self._prepare_dataframe(df, db_columns, pkeys, drop_duplicates)
            columns = df.columns.tolist()

        if columns:
            self._check_columns(columns, db_columns, table)

        rows = await self._insert_data(
            table=table,
            schema=schema,
            df=df,
            csv=csv,
            columns=columns,
            pkeys=pkeys,
            on_conflict=on_conflict,
            force_nulls=force_nulls,
            recompress_after=recompress_after,
        )
        return rows

    async def insert_light(
        self,
        csv: str,
        table: str,
        columns,
        pkeys=None,
        force_nulls: bool = False,
        recompress_after: bool = False,
        schema: str = DEFAULT_SCHEMA,
        on_conflict: ON_CONFLICT = ON_CONFLICT.UPDATE,
    ):
        """Inserts a csv into a table.
        Ideally to call with the results of df2insertlight

        Parameters
        ----------
        csv : string
            csv with the data to insert
        table : str
            Name of the table to insert into
        schema : str, optional
            Schema of the table, by default primarydata
        pkeys: list
            List of Primary keys of the table
        columns: list
            List of the columns of the table.
        on_conflict : ON_CONFLICT, optional
            What to do on conflict, possible values are:
            - 'do_nothing'
            - 'update'
            By default 'do_nothing'
        force_nulls : bool, optional
            If false, null values in the passed dataframe will be ignored when
            updating. If true, null values will be inserted into the database.
            By default False
        recompress_after : bool, optional
            If true, the table chunks will be recompressed after the insert.
            By default True

        Returns
        -------
        int
            Number of rows inserted/updated
        """

        on_conflict = ON_CONFLICT(on_conflict)

        rows = await self._insert_data(
            df=None,
            csv=csv,
            table=table,
            schema=schema,
            pkeys=pkeys,
            columns=columns,
            on_conflict=on_conflict,
            force_nulls=force_nulls,
            recompress_after=recompress_after,
        )
        return rows

    async def df2insertlight(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = DEFAULT_SCHEMA,
        delimiter="\t",
        drop_duplicates: bool = True,
    ):
        """Given a dataframe and a table it prepares the needed objects to call
        insert_light.
        """
        columns, pkeys = await self._get_columns_info(table, schema)
        df = self._prepare_dataframe(df, columns, pkeys, drop_duplicates)
        return {
            "csv": df.to_csv(
                index=False, header=False, sep=delimiter, quotechar="🖯"
            ).replace("🖯", "$🖯$"),
            "table": table,
            "schema": schema,
            "columns": columns,
            "pkeys": pkeys,
        }

    def insert_sync(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = DEFAULT_SCHEMA,
        on_conflict: ON_CONFLICT = ON_CONFLICT.DO_NOTHING,
    ):
        """Inserts a dataframe into a table. This is the Ssynchronous
            version of insert().

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe to insert
        table : str
            Name of the table to insert into
        schema : str, optional
            Schema of the table, by default primarydata
        on_conflict : ON_CONFLICT, optional
            What to do on conflict, possible values are:
            - 'do_nothing'
            - 'update'
            By default 'do_nothing'

        Returns
        -------
        int
            Number of rows inserted/updated
        """
        return asyncio.run(self.insert(df, table, schema, on_conflict))

    async def _run_backfilling_procedure(
        self,
        table: str,
        schema: str,
        df: pd.DataFrame,
        csv: str,
        columns: list[str],
        pkeys: list[str],
        on_conflict: ON_CONFLICT,
        force_nulls: bool,
        recompress_after: bool,
    ):
        LOG.debug(f"Backfilling into {schema}.{table}")

        await self._copy_to_tmp_table(
            table=table,
            schema=schema,
            df=df,
            csv=csv,
            columns=columns,
            on_commit_drop=False,
        )
        try:
            rows = await timescale.backfill(
                conn=self.conn,
                staging_table=f"_tmp_{table}",
                table_name=table,
                schema_name=schema,
                pkeys=pkeys,
                columns=columns,
                on_conflict=on_conflict,
                force_nulls=force_nulls,
                recompress_after=recompress_after,
            )
        except (NoDataFound, IndeterminateDatatype):
            LOG.debug(
                f"Backfilling into {schema}.{table} failed. "
                "Trying no hypertables method."
            )

            rows = await self._noht_backfill(
                staging_table=f"_tmp_{table}",
                table_name=table,
                schema_name=schema,
                pkeys=pkeys,
                columns=columns,
                on_conflict=on_conflict,
                force_nulls=force_nulls,
            )
        except Exception as e:
            LOG.error("Error during backfilling", e, extra={"tb": e.__traceback__})
            raise

        return rows

    async def _noht_backfill(
        self,
        staging_table: str,
        table_name: str,
        schema_name: str,
        pkeys: list[str],
        columns: list[str],
        on_conflict: str = ON_CONFLICT.DO_NOTHING,
        force_nulls: bool = False,
    ):
        cols_str = ", ".join('"{}"'.format(k) for k in columns)
        primary_key = ", ".join('"{}"'.format(k) for k in pkeys)

        nonkeycols = [a for a in columns if a not in pkeys]

        if on_conflict == ON_CONFLICT.DO_NOTHING:
            onconflict = """DO NOTHING"""

        else:
            if force_nulls:
                updaterow = ",\n".join(
                    [f"{col} = excluded.{col}" for col in nonkeycols]
                )
            else:
                updaterow = ",\n".join(
                    [
                        f"{col} = COALESCE(excluded.{col}, {table_name}.{col})"
                        for col in nonkeycols
                    ]
                )
            wherestr = """WHERE {0}""".format(
                " AND ".join(
                    [f"{table_name}.{keycol} = excluded.{keycol}" for keycol in pkeys]
                )
            )
            onconflict = f"""
                DO UPDATE SET
                {updaterow}
                {wherestr}
                """

        insertstr = f"""
            INSERT INTO {schema_name}.{table_name}({cols_str})
            SELECT {cols_str}
            FROM {staging_table}
            ON CONFLICT ({primary_key})
            {onconflict}
            RETURNING {pkeys[0] if pkeys else "1"};
            """

        try:
            modrows = await self.conn.fetch(insertstr)
            return len(modrows)
        except Exception:
            LOG.debug(
                f"Error while the no hypertable backfilling "
                f"for {schema_name}.{table_name}"
            )
            return 0

    async def _copy_to_tmp_table(
        self,
        table: str,
        schema: str,
        df: pd.DataFrame = None,
        csv: str = None,
        columns: list[str] = None,
        on_commit_drop=False,
    ):
        create_tmp_sql = f""" 
            DROP TABLE IF EXISTS _tmp_{table};
            CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_{table} 
                (LIKE {schema}.{table} INCLUDING DEFAULTS)
            """
        if on_commit_drop:
            create_tmp_sql += " ON COMMIT DROP "
        await self.conn.execute(create_tmp_sql)

        await self.conn.copy_to_table(
            table_name=f"_tmp_{table}", df=df, csv=csv, columns=columns
        )
