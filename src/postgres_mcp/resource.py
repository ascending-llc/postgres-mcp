import logging
from typing import Any, Dict, List, Optional

import mcp.types as types

from .sql import SafeSqlDriver
from .utils.reponse import format_error_response, format_text_response
from .utils.sql_driver import get_sql_driver_for_database

logger = logging.getLogger(__name__)

# Type alias for response format
ResponseType = List[types.TextContent | types.ImageContent | types.EmbeddedResource]


def dynamically_register_resources(mcp_instance, database_name: Optional[str] = None):  # type: ignore
    """
    Register consolidated resource handlers with the MCP instance.
    
    Args:
        mcp_instance: The FastMCP instance to register resources with
        database_name: Optional specific database name. If None, registers dynamic resources.
    """
    
    if database_name:
        logger.info(f"Registering static resources for database: {database_name}")
        _register_static_resources(mcp_instance, database_name)
    else:
        logger.info("Registering dynamic resources with database name parameter")
        _register_dynamic_resources(mcp_instance)


def _register_static_resources(mcp_instance, db_name: str):  # type: ignore
    """Register static resource paths for a specific database."""
    
    tables_uri = f"postgres://database/{db_name}/tables"
    views_uri = f"postgres://database/{db_name}/views"
    
    @mcp_instance.resource(tables_uri)  # type: ignore
    async def get_database_tables_static() -> ResponseType:
        """
        Get comprehensive information about all tables in the configured database.
        
        Returns complete table information including schemas, columns with comments,
        constraints, indexes, and statistics.
        """
        return await _get_tables_impl(db_name)
    
    @mcp_instance.resource(views_uri)  # type: ignore
    async def get_database_views_static() -> ResponseType:
        """
        Get comprehensive information about all views in the configured database.
        
        Returns complete view information including schemas, columns with comments,
        view definitions, and dependencies.
        """
        return await _get_views_impl(db_name)
    
    logger.info(f"Registered static resources: {tables_uri}, {views_uri}")


def _register_dynamic_resources(mcp_instance):  # type: ignore
    """Register dynamic resource paths with database name parameter."""
    
    @mcp_instance.resource("postgres://database/{database_name}/tables")  # type: ignore
    async def get_database_tables_dynamic(database_name: str) -> ResponseType:
        """
        Get comprehensive information about all tables in a specific database.
        
        Args:
            database_name: Name of the database to query
            
        Returns complete table information including schemas, columns with comments,
        constraints, indexes, and statistics.
        """
        return await _get_tables_impl(database_name)
    
    @mcp_instance.resource("postgres://database/{database_name}/views")  # type: ignore
    async def get_database_views_dynamic(database_name: str) -> ResponseType:
        """
        Get comprehensive information about all views in a specific database.
        
        Args:
            database_name: Name of the database to query
            
        Returns complete view information including schemas, columns with comments,
        view definitions, and dependencies.
        """
        return await _get_views_impl(database_name)
    
    logger.info("Registered dynamic resources: postgres://database/{database_name}/tables, postgres://database/{database_name}/views")


async def _get_tables_impl(database_name: str) -> ResponseType:
    """
    Implementation for getting comprehensive table information.
    
    Returns:
        - List of all user schemas
        - Complete table information including:
          * Table metadata (schema, name, type)
          * Column details with comments
          * Constraints (primary key, foreign key, unique, check)
          * Indexes with statistics
          * Table size and row count
    """
    try:
        logger.info(f"Getting comprehensive table information for database: {database_name}")
        sql_driver = await get_sql_driver_for_database(database_name)

        # Get all user schemas
        schema_rows = await sql_driver.execute_query(
            """
            SELECT 
                schema_name,
                schema_owner,
                CASE
                    WHEN schema_name LIKE 'pg_%' THEN 'system'
                    WHEN schema_name = 'information_schema' THEN 'system'
                    ELSE 'user'
                END as schema_type
            FROM information_schema.schemata
            WHERE schema_name NOT LIKE 'pg_%'
              AND schema_name != 'information_schema'
            ORDER BY schema_name
            """
        )
        schemas = [row.cells for row in schema_rows] if schema_rows else []

        # Get all tables with metadata
        table_rows = await sql_driver.execute_query(
            """
            SELECT 
                t.table_schema, 
                t.table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))) as table_size,
                (SELECT reltuples::bigint 
                 FROM pg_class c 
                 JOIN pg_namespace n ON n.oid = c.relnamespace 
                 WHERE n.nspname = t.table_schema AND c.relname = t.table_name) as estimated_rows,
                obj_description((quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass) as table_comment
            FROM information_schema.tables t
            WHERE t.table_type = 'BASE TABLE'
              AND t.table_schema NOT LIKE 'pg_%'
              AND t.table_schema != 'information_schema'
            ORDER BY t.table_schema, t.table_name
            """
        )

        if not table_rows:
            return format_text_response({
                "database": database_name,
                "schemas": schemas,
                "tables": []
            })

        tables_info = []
        for row in table_rows:
            schema_name = row.cells["table_schema"]
            table_name = row.cells["table_name"]

            try:
                # Get columns with comments
                col_rows = await SafeSqlDriver.execute_param_query(
                    sql_driver,
                    """
                    SELECT 
                        c.column_name,
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        c.ordinal_position,
                        c.character_maximum_length,
                        c.numeric_precision,
                        c.numeric_scale,
                        pgd.description as column_comment
                    FROM information_schema.columns c
                    LEFT JOIN pg_catalog.pg_statio_all_tables psat 
                        ON c.table_schema = psat.schemaname AND c.table_name = psat.relname
                    LEFT JOIN pg_catalog.pg_description pgd 
                        ON psat.relid = pgd.objoid AND c.ordinal_position = pgd.objsubid
                    WHERE c.table_schema = {} AND c.table_name = {}
                    ORDER BY c.ordinal_position
                    """,
                    [schema_name, table_name],
                )

                columns = []
                if col_rows:
                    for r in col_rows:
                        col_info = {
                            "name": r.cells["column_name"],
                            "data_type": r.cells["data_type"],
                            "is_nullable": r.cells["is_nullable"],
                            "default": r.cells["column_default"],
                            "position": r.cells["ordinal_position"],
                            "comment": r.cells.get("column_comment", "")
                        }
                        # Add type-specific details
                        if r.cells.get("character_maximum_length"):
                            col_info["max_length"] = r.cells["character_maximum_length"]
                        if r.cells.get("numeric_precision"):
                            col_info["precision"] = r.cells["numeric_precision"]
                        if r.cells.get("numeric_scale"):
                            col_info["scale"] = r.cells["numeric_scale"]
                        columns.append(col_info)

                # Get constraints
                con_rows = await SafeSqlDriver.execute_param_query(
                    sql_driver,
                    """
                    SELECT 
                        tc.constraint_name,
                        tc.constraint_type,
                        kcu.column_name,
                        CASE 
                            WHEN tc.constraint_type = 'FOREIGN KEY' THEN ccu.table_schema
                            ELSE NULL 
                        END as foreign_table_schema,
                        CASE 
                            WHEN tc.constraint_type = 'FOREIGN KEY' THEN ccu.table_name
                            ELSE NULL 
                        END as foreign_table_name,
                        CASE 
                            WHEN tc.constraint_type = 'FOREIGN KEY' THEN ccu.column_name
                            ELSE NULL 
                        END as foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    LEFT JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    LEFT JOIN information_schema.constraint_column_usage AS ccu
                        ON tc.constraint_name = ccu.constraint_name
                        AND tc.constraint_type = 'FOREIGN KEY'
                    WHERE tc.table_schema = {} AND tc.table_name = {}
                    ORDER BY tc.constraint_type, tc.constraint_name, kcu.ordinal_position
                    """,
                    [schema_name, table_name],
                )

                constraints = {}
                if con_rows:
                    for con_row in con_rows:
                        cname = con_row.cells["constraint_name"]
                        ctype = con_row.cells["constraint_type"]
                        col = con_row.cells["column_name"]

                        if cname not in constraints:
                            constraints[cname] = {
                                "type": ctype,
                                "columns": []
                            }
                            # Add foreign key reference info
                            if ctype == "FOREIGN KEY" and con_row.cells.get("foreign_table_name"):
                                constraints[cname]["references"] = {
                                    "schema": con_row.cells["foreign_table_schema"],
                                    "table": con_row.cells["foreign_table_name"],
                                    "column": con_row.cells["foreign_column_name"]
                                }
                        if col:
                            constraints[cname]["columns"].append(col)

                constraints_list = [{"name": name, **data} for name, data in constraints.items()]

                # Get indexes with details
                idx_rows = await SafeSqlDriver.execute_param_query(
                    sql_driver,
                    """
                    SELECT 
                        i.indexname,
                        i.indexdef,
                        pg_size_pretty(pg_relation_size(quote_ident(i.schemaname) || '.' || quote_ident(i.indexname))) as index_size,
                        idx.indisunique as is_unique,
                        idx.indisprimary as is_primary
                    FROM pg_indexes i
                    JOIN pg_class c ON c.relname = i.indexname
                    JOIN pg_index idx ON idx.indexrelid = c.oid
                    WHERE i.schemaname = {} AND i.tablename = {}
                    ORDER BY i.indexname
                    """,
                    [schema_name, table_name],
                )

                indexes = []
                if idx_rows:
                    for idx_row in idx_rows:
                        indexes.append({
                            "name": idx_row.cells["indexname"],
                            "definition": idx_row.cells["indexdef"],
                            "size": idx_row.cells["index_size"],
                            "is_unique": idx_row.cells["is_unique"],
                            "is_primary": idx_row.cells["is_primary"]
                        })

                table_info = {
                    "schema": schema_name,
                    "name": table_name,
                    "type": "table",
                    "comment": row.cells.get("table_comment", ""),
                    "size": row.cells.get("table_size", ""),
                    "estimated_rows": row.cells.get("estimated_rows", 0),
                    "columns": columns,
                    "constraints": constraints_list,
                    "indexes": indexes
                }

                tables_info.append(table_info)

            except Exception as e:
                logger.error(f"Error getting schema for table {database_name}.{schema_name}.{table_name}: {e}")
                # Continue with other tables even if one fails

        result = {
            "database": database_name,
            "schemas": schemas,
            "tables": tables_info,
            "total_tables": len(tables_info)
        }

        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting tables information for database {database_name}: {e}")
        return format_error_response(str(e))


async def _get_views_impl(database_name: str) -> ResponseType:
    """
    Implementation for getting comprehensive view information.
    
    Returns:
        - List of all user schemas
        - Complete view information including:
          * View metadata (schema, name, type)
          * Column details with comments
          * View definition (SQL)
          * Dependent objects
    
    """
    try:
        logger.info(f"Getting comprehensive view information for database: {database_name}")
        sql_driver = await get_sql_driver_for_database(database_name)

        # Get all user schemas
        schema_rows = await sql_driver.execute_query(
            """
            SELECT 
                schema_name,
                schema_owner,
                CASE
                    WHEN schema_name LIKE 'pg_%' THEN 'system'
                    WHEN schema_name = 'information_schema' THEN 'system'
                    ELSE 'user'
                END as schema_type
            FROM information_schema.schemata
            WHERE schema_name NOT LIKE 'pg_%'
              AND schema_name != 'information_schema'
            ORDER BY schema_name
            """
        )
        schemas = [row.cells for row in schema_rows] if schema_rows else []

        # Get all views with metadata
        view_rows = await sql_driver.execute_query(
            """
            SELECT 
                t.table_schema, 
                t.table_name,
                v.view_definition,
                obj_description((quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass) as view_comment
            FROM information_schema.tables t
            LEFT JOIN information_schema.views v 
                ON t.table_schema = v.table_schema AND t.table_name = v.table_name
            WHERE t.table_type = 'VIEW'
              AND t.table_schema NOT LIKE 'pg_%'
              AND t.table_schema != 'information_schema'
            ORDER BY t.table_schema, t.table_name
            """
        )

        if not view_rows:
            return format_text_response({
                "database": database_name,
                "schemas": schemas,
                "views": []
            })

        views_info = []
        for row in view_rows:
            schema_name = row.cells["table_schema"]
            view_name = row.cells["table_name"]

            try:
                # Get columns with comments
                col_rows = await SafeSqlDriver.execute_param_query(
                    sql_driver,
                    """
                    SELECT 
                        c.column_name,
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        c.ordinal_position,
                        c.character_maximum_length,
                        c.numeric_precision,
                        c.numeric_scale,
                        pgd.description as column_comment
                    FROM information_schema.columns c
                    LEFT JOIN pg_catalog.pg_statio_all_tables psat 
                        ON c.table_schema = psat.schemaname AND c.table_name = psat.relname
                    LEFT JOIN pg_catalog.pg_description pgd 
                        ON psat.relid = pgd.objoid AND c.ordinal_position = pgd.objsubid
                    WHERE c.table_schema = {} AND c.table_name = {}
                    ORDER BY c.ordinal_position
                    """,
                    [schema_name, view_name],
                )

                columns = []
                if col_rows:
                    for r in col_rows:
                        col_info = {
                            "name": r.cells["column_name"],
                            "data_type": r.cells["data_type"],
                            "is_nullable": r.cells["is_nullable"],
                            "default": r.cells["column_default"],
                            "position": r.cells["ordinal_position"],
                            "comment": r.cells.get("column_comment", "")
                        }
                        # Add type-specific details
                        if r.cells.get("character_maximum_length"):
                            col_info["max_length"] = r.cells["character_maximum_length"]
                        if r.cells.get("numeric_precision"):
                            col_info["precision"] = r.cells["numeric_precision"]
                        if r.cells.get("numeric_scale"):
                            col_info["scale"] = r.cells["numeric_scale"]
                        columns.append(col_info)

                # Get dependent objects (what tables this view depends on)
                dep_rows = await SafeSqlDriver.execute_param_query(
                    sql_driver,
                    """
                    SELECT DISTINCT
                        source_ns.nspname as source_schema,
                        source_table.relname as source_table,
                        source_table.relkind as source_type
                    FROM pg_depend d
                    JOIN pg_rewrite r ON r.oid = d.objid
                    JOIN pg_class view_class ON view_class.oid = r.ev_class
                    JOIN pg_namespace view_ns ON view_ns.oid = view_class.relnamespace
                    JOIN pg_class source_table ON source_table.oid = d.refobjid
                    JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
                    WHERE view_ns.nspname = {}
                      AND view_class.relname = {}
                      AND source_table.relkind IN ('r', 'v', 'm')
                      AND d.deptype = 'n'
                    """,
                    [schema_name, view_name],
                )

                dependencies = []
                if dep_rows:
                    for dep_row in dep_rows:
                        dep_type_map = {'r': 'table', 'v': 'view', 'm': 'materialized view'}
                        dependencies.append({
                            "schema": dep_row.cells["source_schema"],
                            "name": dep_row.cells["source_table"],
                            "type": dep_type_map.get(dep_row.cells["source_type"], "unknown")
                        })

                view_info = {
                    "schema": schema_name,
                    "name": view_name,
                    "type": "view",
                    "comment": row.cells.get("view_comment", ""),
                    "definition": row.cells.get("view_definition", ""),
                    "columns": columns,
                    "dependencies": dependencies
                }

                views_info.append(view_info)

            except Exception as e:
                logger.error(f"Error getting schema for view {database_name}.{schema_name}.{view_name}: {e}")
                # Continue with other views even if one fails

        result = {
            "database": database_name,
            "schemas": schemas,
            "views": views_info,
            "total_views": len(views_info)
        }

        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting views information for database {database_name}: {e}")
        return format_error_response(str(e))
