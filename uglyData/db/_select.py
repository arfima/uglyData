from typing import Any
from psycopg import sql

from ..utils import is_list_of_tuples


def get_fields_expression(fields: list[str]) -> sql.SQL:
    """Build the expression for selecting fields."""
    if fields is None:
        return sql.SQL("*")
    return sql.SQL(", ").join(sql.Identifier(f) for f in fields)


def split_table_name(table: str) -> tuple[str, str]:
    """Split the table name into schema and table parts."""
    if "." in table:
        return table.split(".")
    return None, table


def build_select_query(fields: sql.SQL, schema: str, table: str) -> sql.SQL:
    """Build the SELECT query."""
    return sql.SQL("SELECT {fields} FROM {table}").format(
        fields=fields,
        table=sql.Identifier(schema, table),
    )


def get_filter_contition(key, filters):
    value = filters[key]
    if isinstance(value, list):
        return sql.Composed(
            [
                sql.Identifier(key),
                sql.SQL(" = "),
                sql.SQL("ANY("),
                sql.Placeholder(),
                sql.SQL(")"),
            ]
        )
    else:
        return sql.Composed([sql.Identifier(key), sql.SQL(" = "), sql.Placeholder()])


def add_json_filter(conditions, filters):
    json_params = []

    for k in filters.get(">>>", []):
        col = k["column"]
        field = k["field"]
        vals = k["value"]

        if isinstance(vals, list):
            conditions.append(
                sql.Composed(
                    [
                        sql.Identifier(col),
                        sql.SQL(" ->> "),
                        sql.Placeholder(),
                        sql.SQL(" ILIKE "),
                        sql.SQL("ANY("),
                        sql.Placeholder(),
                        sql.SQL(")"),
                    ]
                )
            )
            json_params.extend([field, vals])
        else:
            conditions.append(
                sql.Composed(
                    [
                        sql.Identifier(col),
                        sql.SQL(" ->> "),
                        sql.Placeholder(),
                        sql.SQL(" ILIKE "),
                        sql.Placeholder(),
                    ]
                )
            )
            json_params.extend([field, vals])

    return conditions, json_params


def add_filters(
    query: sql.SQL, filters: dict[str, Any], params: list[Any]
) -> tuple[sql.SQL, list[Any]]:
    """Add the filter conditions to the query."""
    if filters:
        conditions = [
            get_filter_contition(k, filters) for k in filters.keys() if k != ">>>"
        ]
        conditions, json_params = add_json_filter(conditions, filters)
        query += sql.SQL(" AND ").join(conditions)
        for values in [v for k, v in filters.items() if k != ">>>"]:
            if isinstance(values, list):
                params += [values]  # * len(values)
            elif isinstance(values, tuple):
                params.append(values[0])

            else:
                params.append(values)

        params.extend(json_params)
    return query, params


def add_search_condition(
    query: sql.SQL, search_query: str, search_columns: list[str], params: list[Any]
) -> sql.SQL:
    """Add the search condition to the query."""
    if search_query:
        if is_list_of_tuples(search_columns):
            cols = [col for col, _ in search_columns]
        else:
            cols = search_columns
        if not search_columns:
            raise ValueError(
                "You must specify the columns to search in with search_columns"
            )
        conditions = [
            sql.SQL("LOWER(")
            + sql.Identifier(col)
            + sql.SQL("::text) LIKE ")
            + sql.Placeholder()
            for col in cols
        ]
        query += sql.SQL(" OR ").join(conditions)
        if "*" in search_query:
            search_term = search_query.replace("*", "%")
        else:
            search_term = f"%{search_query}%"
        params += [search_term.lower()] * len(search_columns)
    return query, params


def check_sorting(sorting: list[str]) -> None:
    """Sorting must be a list of string with : spliting columna name and order.
    Order must be asc or desc. If not provided, asc is the default order."""

    for i, sort in enumerate(sorting):
        if not isinstance(sort, str):
            raise TypeError("Sorting must be a list of string")
        if ":" not in sort:
            sort += ":asc"
        column, order = sort.split(":")
        if order not in ["asc", "desc"]:
            raise ValueError("Order must be asc or desc")
        sorting[i] = (column, order)
    return sorting


def add_sorting(
    query: sql.SQL,
    params: list,
    search_query: str,
    search_columns: list[str],
    sorting: list[str],
) -> sql.SQL:
    """Add the sorting condition to the query."""
    is_weighted_search = search_query and is_list_of_tuples(search_columns)
    if sorting or is_weighted_search:
        query += sql.SQL(" ORDER BY ")
        if is_weighted_search:
            conditions = [
                sql.SQL("LOWER(")
                + sql.Identifier(col)
                + sql.SQL("::text) LIKE ")
                + sql.Placeholder()
                + sql.SQL(" THEN ")
                + sql.Literal(weight)
                for col, weight in search_columns
            ]
            query += (
                sql.SQL("(CASE WHEN ")
                + sql.SQL(" WHEN ").join(conditions)
                + sql.SQL(" ELSE 0 END) DESC")
            )
            params += [f"%{search_query.lower()}%"] * len(search_columns)
        if sorting:
            sorting = check_sorting(sorting)
            if is_weighted_search:
                query += sql.SQL(", ")
            sorting_conditions = [
                sql.Identifier(col) + sql.SQL(" ") + sql.SQL(order)
                for col, order in sorting
            ]
            query += sql.SQL(", ").join(sorting_conditions)
    return query, params


def add_limit_and_offset(query: sql.SQL, limit: int, offset: int) -> sql.SQL:
    """Add the limit and offset conditions to the query."""
    if limit is not None:
        query += sql.SQL(" LIMIT ") + sql.Literal(limit)
    if offset is not None:
        query += sql.SQL(" OFFSET ") + sql.Literal(offset)
    return query
