"""
Script to copy views from one SQL Server database to another.

This script connects to a source database, retrieves view definitions,
and recreates them in a target database.

Environment Variables Required:
- SOURCE_SQL_SERVER: Source server name
- SOURCE_SQL_DATABASE: Source database name
- SOURCE_SQL_USERNAME: Source username
- SOURCE_SQL_PASSWORD: Source password
- TARGET_SQL_SERVER: Target server name
- TARGET_SQL_DATABASE: Target database name
- TARGET_SQL_USERNAME: Target username
- TARGET_SQL_PASSWORD: Target password

Optional:
- SPECIFIC_VIEWS: Comma-separated list of view names (if not set, copies all views)
"""

import pyodbc
import os
import logging
from typing import List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_connection_string(server: str, database: str, username: str, password: str) -> str:
    """Build SQL Server connection string"""
    return (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};'
        f'DATABASE={database};UID={username};PWD={password};'
        f'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;'
    )

def get_source_connection():
    """Get connection to source database"""
    server = os.environ.get('SOURCE_SQL_SERVER')
    database = os.environ.get('SOURCE_SQL_DATABASE')
    username = os.environ.get('SOURCE_SQL_USERNAME')
    password = os.environ.get('SOURCE_SQL_PASSWORD')

    if not all([server, database, username, password]):
        raise ValueError("Missing source database credentials in environment variables")

    conn_string = get_connection_string(server, database, username, password)
    return pyodbc.connect(conn_string)

def get_target_connection():
    """Get connection to target database"""
    server = os.environ.get('TARGET_SQL_SERVER')
    database = os.environ.get('TARGET_SQL_DATABASE')
    username = os.environ.get('TARGET_SQL_USERNAME')
    password = os.environ.get('TARGET_SQL_PASSWORD')

    if not all([server, database, username, password]):
        raise ValueError("Missing target database credentials in environment variables")

    conn_string = get_connection_string(server, database, username, password)
    return pyodbc.connect(conn_string)

def get_view_list(cursor, specific_views: Optional[List[str]] = None) -> List[Tuple[str, str]]:
    """
    Get list of views from the database
    Returns: List of tuples (schema_name, view_name)
    """
    query = """
        SELECT
            SCHEMA_NAME(v.schema_id) AS SchemaName,
            v.name AS ViewName
        FROM sys.views v
        WHERE v.is_ms_shipped = 0  -- Exclude system views
    """

    if specific_views:
        placeholders = ','.join(['?' for _ in specific_views])
        query += f" AND v.name IN ({placeholders})"
        cursor.execute(query, specific_views)
    else:
        cursor.execute(query)

    return [(row.SchemaName, row.ViewName) for row in cursor.fetchall()]

def get_view_definition(cursor, schema_name: str, view_name: str) -> str:
    """Get the CREATE VIEW statement for a specific view"""
    query = """
        SELECT m.definition
        FROM sys.views v
        INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
        WHERE SCHEMA_NAME(v.schema_id) = ?
        AND v.name = ?
    """
    cursor.execute(query, (schema_name, view_name))
    result = cursor.fetchone()

    if result:
        definition = result[0]
        # Remove leading/trailing whitespace
        definition = definition.strip()

        # If the definition doesn't start with CREATE, add it
        if not definition.upper().startswith('CREATE'):
            definition = f"CREATE VIEW [{schema_name}].[{view_name}] AS\n{definition}"

        return definition
    else:
        raise ValueError(f"View {schema_name}.{view_name} not found")

def drop_view_if_exists(cursor, schema_name: str, view_name: str):
    """Drop view if it exists in target database"""
    drop_sql = f"""
        IF OBJECT_ID('[{schema_name}].[{view_name}]', 'V') IS NOT NULL
        DROP VIEW [{schema_name}].[{view_name}];
    """
    try:
        cursor.execute(drop_sql)
        logging.info(f"Dropped existing view {schema_name}.{view_name}")
    except Exception as e:
        logging.warning(f"Could not drop view {schema_name}.{view_name}: {e}")

def create_schema_if_not_exists(cursor, schema_name: str):
    """Create schema in target database if it doesn't exist"""
    if schema_name.lower() in ['dbo', 'sys', 'information_schema']:
        return  # Skip system schemas

    check_sql = f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema_name}')
        BEGIN
            EXEC('CREATE SCHEMA [{schema_name}]')
        END
    """
    try:
        cursor.execute(check_sql)
        logging.info(f"Ensured schema {schema_name} exists")
    except Exception as e:
        logging.warning(f"Could not create schema {schema_name}: {e}")

def create_view(cursor, schema_name: str, view_name: str, definition: str):
    """Create view in target database"""
    try:
        cursor.execute(definition)
        logging.info(f"Created view {schema_name}.{view_name}")
        return True
    except Exception as e:
        logging.error(f"Error creating view {schema_name}.{view_name}: {e}")
        logging.error(f"Definition was:\n{definition}")
        return False

def copy_views(specific_views: Optional[List[str]] = None, dry_run: bool = False):
    """
    Main function to copy views from source to target database

    Args:
        specific_views: List of specific view names to copy (None = all views)
        dry_run: If True, only show what would be done without making changes
    """
    source_conn = None
    target_conn = None

    try:
        # Connect to source database
        logging.info("Connecting to source database...")
        source_conn = get_source_connection()
        source_cursor = source_conn.cursor()
        logging.info("Connected to source database")

        # Get list of views to copy
        logging.info("Fetching view list from source database...")
        views = get_view_list(source_cursor, specific_views)
        logging.info(f"Found {len(views)} views to copy")

        if not views:
            logging.warning("No views found to copy")
            return

        # Connect to target database
        if not dry_run:
            logging.info("Connecting to target database...")
            target_conn = get_target_connection()
            target_cursor = target_conn.cursor()
            logging.info("Connected to target database")

        # Copy each view
        success_count = 0
        fail_count = 0

        for schema_name, view_name in views:
            try:
                logging.info(f"\nProcessing view: {schema_name}.{view_name}")

                # Get view definition from source
                definition = get_view_definition(source_cursor, schema_name, view_name)

                if dry_run:
                    logging.info(f"[DRY RUN] Would create view {schema_name}.{view_name}")
                    logging.info(f"Definition:\n{definition}\n")
                    success_count += 1
                else:
                    # Create schema if needed
                    create_schema_if_not_exists(target_cursor, schema_name)
                    target_conn.commit()

                    # Drop existing view if it exists
                    drop_view_if_exists(target_cursor, schema_name, view_name)
                    target_conn.commit()

                    # Create view in target
                    if create_view(target_cursor, schema_name, view_name, definition):
                        target_conn.commit()
                        success_count += 1
                    else:
                        target_conn.rollback()
                        fail_count += 1

            except Exception as e:
                logging.error(f"Error processing view {schema_name}.{view_name}: {e}")
                if target_conn:
                    target_conn.rollback()
                fail_count += 1

        # Summary
        logging.info(f"\n{'='*60}")
        logging.info(f"SUMMARY:")
        logging.info(f"Total views processed: {len(views)}")
        logging.info(f"Successful: {success_count}")
        logging.info(f"Failed: {fail_count}")
        logging.info(f"{'='*60}")

    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise

    finally:
        # Close connections
        if source_conn:
            source_conn.close()
            logging.info("Source connection closed")
        if target_conn:
            target_conn.close()
            logging.info("Target connection closed")

def main():
    """Main entry point"""
    # Check if specific views are specified in environment variable
    specific_views_str = os.environ.get('SPECIFIC_VIEWS')
    specific_views = None

    if specific_views_str:
        specific_views = [v.strip() for v in specific_views_str.split(',')]
        logging.info(f"Will copy specific views: {specific_views}")
    else:
        logging.info("Will copy all views from source database")

    # Check for dry run mode
    dry_run = os.environ.get('DRY_RUN', 'false').lower() == 'true'
    if dry_run:
        logging.info("Running in DRY RUN mode - no changes will be made")

    # Execute copy
    copy_views(specific_views=specific_views, dry_run=dry_run)

if __name__ == "__main__":
    main()
