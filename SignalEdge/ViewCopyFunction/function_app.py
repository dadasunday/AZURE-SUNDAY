"""
Azure Function to Copy Views Between SQL Server Databases

This function copies view definitions from a source database to a target database.
Can be triggered manually via HTTP or scheduled via timer.

Environment Variables Required:
- SOURCE_SQL_SERVER: Source server name
- SOURCE_SQL_DATABASE: Source database name
- SOURCE_SQL_USERNAME: Source username
- SOURCE_SQL_PASSWORD: Source password
- TARGET_SQL_SERVER: Target server name
- TARGET_SQL_DATABASE: Target database name
- TARGET_SQL_USERNAME: Target username
- TARGET_SQL_PASSWORD: Target password

Optional Environment Variables:
- SPECIFIC_VIEWS: Comma-separated list of view names (if not set, copies all views)
- DROP_EXISTING_VIEWS: Set to 'true' to drop existing views before creating (default: true)
- CREATE_SCHEMAS: Set to 'true' to create missing schemas (default: true)
- SYNC_SCHEDULE: Cron expression for timer trigger (default: daily at 2 AM)
"""

import azure.functions as func
import logging
import pyodbc
import os
import json
from typing import List, Tuple, Optional, Dict
from datetime import datetime

app = func.FunctionApp()

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
        f'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;LoginTimeout=60;'
    )

def get_source_connection():
    """Get connection to source database"""
    server = os.environ.get('SOURCE_SQL_SERVER')
    database = os.environ.get('SOURCE_SQL_DATABASE')
    username = os.environ.get('SOURCE_SQL_USERNAME')
    password = os.environ.get('SOURCE_SQL_PASSWORD')

    if not all([server, database, username, password]):
        raise ValueError("Missing source database credentials in environment variables")

    logging.info(f"Connecting to source: {server}/{database}")
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

    logging.info(f"Connecting to target: {server}/{database}")
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
        definition = definition.strip()

        # Ensure definition starts with CREATE VIEW
        if not definition.upper().startswith('CREATE'):
            definition = f"CREATE VIEW [{schema_name}].[{view_name}] AS\n{definition}"

        return definition
    else:
        raise ValueError(f"View {schema_name}.{view_name} not found")

def get_view_dependencies(cursor, schema_name: str, view_name: str) -> List[str]:
    """Get list of objects that a view depends on"""
    query = """
        SELECT DISTINCT
            SCHEMA_NAME(o.schema_id) + '.' + o.name AS DependentObject
        FROM sys.views v
        INNER JOIN sys.sql_expression_dependencies sed ON v.object_id = sed.referencing_id
        INNER JOIN sys.objects o ON sed.referenced_id = o.object_id
        WHERE SCHEMA_NAME(v.schema_id) = ?
        AND v.name = ?
        ORDER BY DependentObject
    """
    try:
        cursor.execute(query, (schema_name, view_name))
        return [row.DependentObject for row in cursor.fetchall()]
    except Exception as e:
        logging.warning(f"Could not get dependencies for {schema_name}.{view_name}: {e}")
        return []

def drop_view_if_exists(cursor, schema_name: str, view_name: str) -> bool:
    """Drop view if it exists in target database"""
    drop_sql = f"""
        IF OBJECT_ID('[{schema_name}].[{view_name}]', 'V') IS NOT NULL
        DROP VIEW [{schema_name}].[{view_name}];
    """
    try:
        cursor.execute(drop_sql)
        logging.info(f"Dropped existing view {schema_name}.{view_name}")
        return True
    except Exception as e:
        logging.error(f"Could not drop view {schema_name}.{view_name}: {e}")
        return False

def create_schema_if_not_exists(cursor, schema_name: str) -> bool:
    """Create schema in target database if it doesn't exist"""
    if schema_name.lower() in ['dbo', 'sys', 'information_schema']:
        return True  # Skip system schemas

    check_sql = f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema_name}')
        BEGIN
            EXEC('CREATE SCHEMA [{schema_name}]')
        END
    """
    try:
        cursor.execute(check_sql)
        logging.info(f"Ensured schema {schema_name} exists in target")
        return True
    except Exception as e:
        logging.error(f"Could not create schema {schema_name}: {e}")
        return False

def create_view(cursor, schema_name: str, view_name: str, definition: str) -> bool:
    """Create view in target database"""
    try:
        cursor.execute(definition)
        logging.info(f"✓ Created view {schema_name}.{view_name}")
        return True
    except Exception as e:
        logging.error(f"✗ Error creating view {schema_name}.{view_name}: {e}")
        logging.debug(f"Definition was:\n{definition}")
        return False

def copy_views_operation() -> Dict:
    """
    Main function to copy views from source to target database
    Returns summary dictionary with results
    """
    source_conn = None
    target_conn = None
    summary = {
        "start_time": datetime.utcnow().isoformat(),
        "source_database": f"{os.environ.get('SOURCE_SQL_SERVER')}/{os.environ.get('SOURCE_SQL_DATABASE')}",
        "target_database": f"{os.environ.get('TARGET_SQL_SERVER')}/{os.environ.get('TARGET_SQL_DATABASE')}",
        "total_views": 0,
        "successful": 0,
        "failed": 0,
        "skipped": 0,
        "view_details": [],
        "errors": []
    }

    try:
        # Get configuration
        specific_views_str = os.environ.get('SPECIFIC_VIEWS')
        specific_views = None
        if specific_views_str:
            specific_views = [v.strip() for v in specific_views_str.split(',')]
            logging.info(f"Will copy specific views: {specific_views}")

        drop_existing = os.environ.get('DROP_EXISTING_VIEWS', 'true').lower() == 'true'
        create_schemas = os.environ.get('CREATE_SCHEMAS', 'true').lower() == 'true'

        # Connect to databases
        logging.info("=" * 60)
        logging.info("STARTING VIEW COPY OPERATION")
        logging.info("=" * 60)

        source_conn = get_source_connection()
        source_cursor = source_conn.cursor()
        logging.info("✓ Connected to source database")

        target_conn = get_target_connection()
        target_cursor = target_conn.cursor()
        logging.info("✓ Connected to target database")

        # Get list of views to copy
        logging.info("\nFetching view list from source database...")
        views = get_view_list(source_cursor, specific_views)
        summary["total_views"] = len(views)
        logging.info(f"Found {len(views)} views to copy")

        if not views:
            logging.warning("No views found to copy")
            summary["errors"].append("No views found in source database")
            return summary

        # Copy each view
        for idx, (schema_name, view_name) in enumerate(views, 1):
            view_result = {
                "schema": schema_name,
                "view": view_name,
                "status": "pending",
                "error": None,
                "dependencies": []
            }

            try:
                logging.info(f"\n[{idx}/{len(views)}] Processing view: {schema_name}.{view_name}")

                # Get view definition from source
                definition = get_view_definition(source_cursor, schema_name, view_name)

                # Get dependencies (for logging purposes)
                dependencies = get_view_dependencies(source_cursor, schema_name, view_name)
                if dependencies:
                    logging.info(f"  Dependencies: {', '.join(dependencies)}")
                    view_result["dependencies"] = dependencies

                # Create schema if needed
                if create_schemas:
                    if not create_schema_if_not_exists(target_cursor, schema_name):
                        view_result["status"] = "failed"
                        view_result["error"] = "Could not create schema"
                        summary["failed"] += 1
                        summary["view_details"].append(view_result)
                        target_conn.rollback()
                        continue
                    target_conn.commit()

                # Drop existing view if configured
                if drop_existing:
                    drop_view_if_exists(target_cursor, schema_name, view_name)
                    target_conn.commit()

                # Create view in target
                if create_view(target_cursor, schema_name, view_name, definition):
                    target_conn.commit()
                    view_result["status"] = "success"
                    summary["successful"] += 1
                else:
                    target_conn.rollback()
                    view_result["status"] = "failed"
                    view_result["error"] = "Create view failed"
                    summary["failed"] += 1

            except Exception as e:
                error_msg = str(e)
                logging.error(f"✗ Error processing view {schema_name}.{view_name}: {error_msg}")
                view_result["status"] = "failed"
                view_result["error"] = error_msg
                summary["failed"] += 1
                target_conn.rollback()

            summary["view_details"].append(view_result)

        # Final summary
        summary["end_time"] = datetime.utcnow().isoformat()

        logging.info("\n" + "=" * 60)
        logging.info("SUMMARY")
        logging.info("=" * 60)
        logging.info(f"Total views processed: {summary['total_views']}")
        logging.info(f"✓ Successful: {summary['successful']}")
        logging.info(f"✗ Failed: {summary['failed']}")
        logging.info(f"Source: {summary['source_database']}")
        logging.info(f"Target: {summary['target_database']}")
        logging.info("=" * 60)

        return summary

    except Exception as e:
        error_msg = f"Fatal error during view copy operation: {str(e)}"
        logging.error(error_msg)
        summary["errors"].append(error_msg)
        summary["end_time"] = datetime.utcnow().isoformat()
        return summary

    finally:
        # Close connections
        if source_conn:
            source_conn.close()
            logging.info("Source connection closed")
        if target_conn:
            target_conn.close()
            logging.info("Target connection closed")


# =============================================================================
# Azure Function Triggers
# =============================================================================

# Timer trigger: runs daily at 2 AM UTC by default
# Change schedule using SYNC_SCHEDULE environment variable
@app.timer_trigger(
    schedule="0 0 2 * * *",  # Daily at 2 AM UTC
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def ViewCopyTimer(myTimer: func.TimerRequest) -> None:
    """Timer-triggered function that runs on a schedule"""
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('ViewCopyFunction timer trigger started.')

    try:
        summary = copy_views_operation()

        # Log summary
        logging.info(f"Timer execution completed: {summary['successful']} succeeded, {summary['failed']} failed")

    except Exception as e:
        logging.error(f"Error in timer trigger: {e}")

    logging.info('ViewCopyFunction timer trigger completed.')


# HTTP trigger for manual execution
@app.route(route="ViewCopy", auth_level=func.AuthLevel.FUNCTION)
def ViewCopyHttp(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP-triggered function for manual execution"""
    logging.info('ViewCopyFunction HTTP trigger started.')

    try:
        # Check for optional parameters in request
        specific_views = req.params.get('views')
        if specific_views:
            os.environ['SPECIFIC_VIEWS'] = specific_views
            logging.info(f"Request specified views: {specific_views}")

        # Execute copy operation
        summary = copy_views_operation()

        # Format response
        response_data = {
            "status": "completed",
            "summary": summary
        }

        # Determine HTTP status code
        if summary["failed"] > 0 and summary["successful"] == 0:
            status_code = 500
            response_data["status"] = "failed"
        elif summary["failed"] > 0:
            status_code = 207  # Multi-status (partial success)
            response_data["status"] = "partial_success"
        else:
            status_code = 200
            response_data["status"] = "success"

        return func.HttpResponse(
            body=json.dumps(response_data, indent=2),
            status_code=status_code,
            mimetype="application/json"
        )

    except Exception as e:
        error_msg = f"Error in HTTP trigger: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            body=json.dumps({
                "status": "error",
                "error": error_msg
            }, indent=2),
            status_code=500,
            mimetype="application/json"
        )


# HTTP trigger to get status/info
@app.route(route="ViewCopy/status", auth_level=func.AuthLevel.FUNCTION)
def ViewCopyStatus(req: func.HttpRequest) -> func.HttpResponse:
    """Get configuration and status information"""
    logging.info('ViewCopyFunction status endpoint called.')

    try:
        config_info = {
            "source_server": os.environ.get('SOURCE_SQL_SERVER', 'NOT_SET'),
            "source_database": os.environ.get('SOURCE_SQL_DATABASE', 'NOT_SET'),
            "target_server": os.environ.get('TARGET_SQL_SERVER', 'NOT_SET'),
            "target_database": os.environ.get('TARGET_SQL_DATABASE', 'NOT_SET'),
            "specific_views": os.environ.get('SPECIFIC_VIEWS', 'ALL'),
            "drop_existing_views": os.environ.get('DROP_EXISTING_VIEWS', 'true'),
            "create_schemas": os.environ.get('CREATE_SCHEMAS', 'true'),
            "sync_schedule": os.environ.get('SYNC_SCHEDULE', '0 0 2 * * *')
        }

        return func.HttpResponse(
            body=json.dumps({
                "status": "operational",
                "configuration": config_info
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(
            body=json.dumps({
                "status": "error",
                "error": str(e)
            }, indent=2),
            status_code=500,
            mimetype="application/json"
        )
