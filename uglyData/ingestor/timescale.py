from ..db import AsyncDB
from enum import Enum

from ..log import get_logger

LOG = get_logger(__name__)


class ON_CONFLICT(str, Enum):
    DO_NOTHING = "do_nothing"
    UPDATE = "update"


async def chunk_compression_stats(conn: AsyncDB, table_name: str, schema_name: str):
    return conn.fetch(
        f"""
            SELECT * FROM chunk_compression_stats('{schema_name}.{table_name}')
        """,
        output="dataframe",
    )


async def backfill(
    conn: AsyncDB,
    staging_table: str,
    table_name: str,
    schema_name: str,
    pkeys: list[str],
    columns: list[str],
    on_conflict: str = ON_CONFLICT.DO_NOTHING,
    force_nulls: bool = False,
    recompress_after: bool = True,
):
    rows = 0
    dest_table = f"{schema_name}.{table_name}"

    if on_conflict == ON_CONFLICT.UPDATE:
        conflict_action = "UPDATE"
    elif on_conflict == ON_CONFLICT.DO_NOTHING:
        conflict_action = "NOTHING"

    update_columns = [col for col in columns if col not in pkeys]

    sql = f"""
        CALL decompress_backfill(
            staging_table=>'{staging_table}', 
            destination_hypertable=>'{dest_table}',
            on_conflict_action=>'{conflict_action}',
            on_conflict_target=>'({", ".join(pkeys)})',
            on_conflict_update_columns=> ARRAY{update_columns},
            force_nulls=> '{force_nulls}',
            recompress_after=>'{recompress_after}'
        );
    """

    try:
        notice_msg = await conn.execute(sql, autocommit=True, notice=True)
    except Exception:
        raise

    try:
        rows = sum(
            [int(message.split(" ")[0]) for message in notice_msg if "rows" in message]
        )
    except Exception as e:
        rows = 0
        LOG.error("Error during backfilling", e, extra={"tb": e.__traceback__})
    return rows


async def get_compression_job(conn: AsyncDB, table_name: str, schema_name: str):
    return conn.fetchval(
        f"""
            SELECT j.job_id
            FROM timescaledb_information.jobs j
            INNER JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
            WHERE j.proc_name = 'policy_compression' 
                    AND s.hypertable_name = '{schema_name}.{table_name}';  
        """
    )


async def run_compression_job(
    conn: AsyncDB, job_id: int = None, table_name=None, schema_name=None
):
    if not job_id and table_name and schema_name:
        job_id = await get_compression_job(conn, table_name, schema_name)
    elif table_name is None or schema_name is None:
        raise ValueError("Must provide either job_id or table_name and schema_name")

    return conn.execute(
        f"""
            SELECT alter_job({job_id}, scheduled => true)
        """
    )


async def pause_compression_job(
    conn: AsyncDB, job_id: int = None, table_name=None, schema_name=None
):
    if not job_id:
        job_id = await get_compression_job(conn, table_name, schema_name)
    return conn.execute(
        f"""
            SELECT alter_job({job_id}, scheduled => false)
        """
    )


async def get_chunks(
    conn: AsyncDB,
    table_name: str,
    schema_name: str,
    columns: list[str] = ["chunk_schema", "chunk_name", "compression_status"],
    output: str = "dataframe",
):
    return await conn.fetch(
        f"""
            SELECT {', '.join(columns)} 
            FROM chunk_compression_stats('{schema_name}.{table_name}')
            ORDER BY chunk_name ASC
        """,
        output=output,
    )
