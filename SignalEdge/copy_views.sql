/*
================================================================================
SQL Script to Copy Views Between Databases
================================================================================

INSTRUCTIONS:
1. Connect to your SOURCE database
2. Run PART 1 to generate CREATE VIEW statements
3. Copy the output from the Results grid
4. Connect to your TARGET database
5. Run PART 2 (the copied CREATE VIEW statements)

CONFIGURATION:
- Adjust the WHERE clause in PART 1 to filter specific views if needed
- By default, this script copies all user-defined views (excludes system views)

================================================================================
*/

-- ============================================================================
-- PART 1: RUN THIS ON SOURCE DATABASE
-- ============================================================================
-- This query generates CREATE VIEW statements for all views in the database

SET NOCOUNT ON;

-- Optional: Filter specific views by uncommenting and modifying this line
-- DECLARE @SpecificViews TABLE (ViewName NVARCHAR(128));
-- INSERT INTO @SpecificViews VALUES ('ViewName1'), ('ViewName2'), ('ViewName3');

PRINT '-- ============================================================================'
PRINT '-- Generated View Definitions'
PRINT '-- Generated on: ' + CONVERT(VARCHAR, GETDATE(), 120)
PRINT '-- Source Database: ' + DB_NAME()
PRINT '-- ============================================================================'
PRINT ''

-- Generate DROP IF EXISTS statements
SELECT
    '-- Drop view if exists: ' + SCHEMA_NAME(v.schema_id) + '.' + v.name AS [-- Script --]
FROM sys.views v
WHERE v.is_ms_shipped = 0
    -- Uncomment to filter specific views:
    -- AND v.name IN (SELECT ViewName FROM @SpecificViews)
ORDER BY SCHEMA_NAME(v.schema_id), v.name;

PRINT ''
PRINT 'GO'
PRINT ''

-- Generate CREATE SCHEMA statements
SELECT DISTINCT
    'IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ''' + SCHEMA_NAME(v.schema_id) + ''')' + CHAR(13) + CHAR(10) +
    'BEGIN' + CHAR(13) + CHAR(10) +
    '    EXEC(''CREATE SCHEMA [' + SCHEMA_NAME(v.schema_id) + ']'')' + CHAR(13) + CHAR(10) +
    'END' AS [-- Script --]
FROM sys.views v
WHERE v.is_ms_shipped = 0
    AND SCHEMA_NAME(v.schema_id) NOT IN ('dbo', 'sys', 'INFORMATION_SCHEMA')
    -- Uncomment to filter specific views:
    -- AND v.name IN (SELECT ViewName FROM @SpecificViews)
ORDER BY SCHEMA_NAME(v.schema_id);

PRINT 'GO'
PRINT ''

-- Generate DROP VIEW statements
SELECT
    'IF OBJECT_ID(''[' + SCHEMA_NAME(v.schema_id) + '].[' + v.name + ']'', ''V'') IS NOT NULL' + CHAR(13) + CHAR(10) +
    '    DROP VIEW [' + SCHEMA_NAME(v.schema_id) + '].[' + v.name + '];' + CHAR(13) + CHAR(10) +
    'GO' AS [-- Script --]
FROM sys.views v
WHERE v.is_ms_shipped = 0
    -- Uncomment to filter specific views:
    -- AND v.name IN (SELECT ViewName FROM @SpecificViews)
ORDER BY SCHEMA_NAME(v.schema_id), v.name;

PRINT ''

-- Generate CREATE VIEW statements
SELECT
    '-- ============================================================================' + CHAR(13) + CHAR(10) +
    '-- View: ' + SCHEMA_NAME(v.schema_id) + '.' + v.name + CHAR(13) + CHAR(10) +
    '-- ============================================================================' + CHAR(13) + CHAR(10) +
    ISNULL(m.definition, 'ERROR: Definition not found') + CHAR(13) + CHAR(10) +
    'GO' + CHAR(13) + CHAR(10) AS [-- Script --]
FROM sys.views v
INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
WHERE v.is_ms_shipped = 0
    -- Uncomment to filter specific views:
    -- AND v.name IN (SELECT ViewName FROM @SpecificViews)
ORDER BY SCHEMA_NAME(v.schema_id), v.name;

PRINT ''
PRINT '-- ============================================================================'
PRINT '-- End of Generated Script'
PRINT '-- ============================================================================'

GO

-- ============================================================================
-- ALTERNATIVE PART 1: Generate script to a single string (for large databases)
-- ============================================================================
-- If you have many views, use this instead to generate one complete script

/*
DECLARE @Script NVARCHAR(MAX) = '';

-- Header
SET @Script = @Script + '-- ============================================================================' + CHAR(13) + CHAR(10);
SET @Script = @Script + '-- Generated View Definitions' + CHAR(13) + CHAR(10);
SET @Script = @Script + '-- Generated on: ' + CONVERT(VARCHAR, GETDATE(), 120) + CHAR(13) + CHAR(10);
SET @Script = @Script + '-- Source Database: ' + DB_NAME() + CHAR(13) + CHAR(10);
SET @Script = @Script + '-- ============================================================================' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10);

-- Create schemas
SELECT @Script = @Script +
    'IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ''' + SCHEMA_NAME(v.schema_id) + ''')' + CHAR(13) + CHAR(10) +
    'BEGIN' + CHAR(13) + CHAR(10) +
    '    EXEC(''CREATE SCHEMA [' + SCHEMA_NAME(v.schema_id) + ']'')' + CHAR(13) + CHAR(10) +
    'END' + CHAR(13) + CHAR(10) +
    'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
FROM sys.views v
WHERE v.is_ms_shipped = 0
    AND SCHEMA_NAME(v.schema_id) NOT IN ('dbo', 'sys', 'INFORMATION_SCHEMA')
GROUP BY SCHEMA_NAME(v.schema_id);

-- Drop views
SELECT @Script = @Script +
    'IF OBJECT_ID(''[' + SCHEMA_NAME(v.schema_id) + '].[' + v.name + ']'', ''V'') IS NOT NULL' + CHAR(13) + CHAR(10) +
    '    DROP VIEW [' + SCHEMA_NAME(v.schema_id) + '].[' + v.name + '];' + CHAR(13) + CHAR(10) +
    'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
FROM sys.views v
WHERE v.is_ms_shipped = 0
ORDER BY SCHEMA_NAME(v.schema_id), v.name;

-- Create views
SELECT @Script = @Script +
    '-- ============================================================================' + CHAR(13) + CHAR(10) +
    '-- View: ' + SCHEMA_NAME(v.schema_id) + '.' + v.name + CHAR(13) + CHAR(10) +
    '-- ============================================================================' + CHAR(13) + CHAR(10) +
    ISNULL(m.definition, 'ERROR: Definition not found') + CHAR(13) + CHAR(10) +
    'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
FROM sys.views v
INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
WHERE v.is_ms_shipped = 0
ORDER BY SCHEMA_NAME(v.schema_id), v.name;

-- Footer
SET @Script = @Script + '-- ============================================================================' + CHAR(13) + CHAR(10);
SET @Script = @Script + '-- End of Generated Script' + CHAR(13) + CHAR(10);
SET @Script = @Script + '-- ============================================================================' + CHAR(13) + CHAR(10);

-- Output the complete script
SELECT @Script AS [Complete Script];

GO
*/

-- ============================================================================
-- PART 2: RUN THE GENERATED SCRIPT ON TARGET DATABASE
-- ============================================================================
-- Copy the output from PART 1 and run it here on the target database
-- The script will:
-- 1. Create any necessary schemas
-- 2. Drop existing views if they exist
-- 3. Create the views with their definitions

-- Example output format (this will be generated by PART 1):
/*

-- ============================================================================
-- View: dbo.YourViewName
-- ============================================================================
CREATE VIEW [dbo].[YourViewName]
AS
    SELECT ...
GO

*/

-- ============================================================================
-- UTILITY QUERIES
-- ============================================================================

-- Query 1: List all views in current database
/*
SELECT
    SCHEMA_NAME(v.schema_id) AS SchemaName,
    v.name AS ViewName,
    v.create_date AS CreatedDate,
    v.modify_date AS ModifiedDate
FROM sys.views v
WHERE v.is_ms_shipped = 0
ORDER BY SchemaName, ViewName;
*/

-- Query 2: Get definition of a specific view
/*
SELECT
    SCHEMA_NAME(v.schema_id) AS SchemaName,
    v.name AS ViewName,
    m.definition AS ViewDefinition
FROM sys.views v
INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
WHERE v.name = 'YourViewName';  -- Replace with your view name
*/

-- Query 3: Find views that depend on a specific table
/*
SELECT DISTINCT
    SCHEMA_NAME(v.schema_id) AS ViewSchema,
    v.name AS ViewName
FROM sys.views v
INNER JOIN sys.sql_expression_dependencies sed ON v.object_id = sed.referencing_id
INNER JOIN sys.tables t ON sed.referenced_id = t.object_id
WHERE t.name = 'YourTableName'  -- Replace with your table name
ORDER BY ViewSchema, ViewName;
*/

-- Query 4: Copy a single view definition
/*
DECLARE @ViewName NVARCHAR(128) = 'YourViewName';  -- Replace with your view name

SELECT
    '-- View: ' + SCHEMA_NAME(v.schema_id) + '.' + v.name + CHAR(13) + CHAR(10) +
    'IF OBJECT_ID(''[' + SCHEMA_NAME(v.schema_id) + '].[' + v.name + ']'', ''V'') IS NOT NULL' + CHAR(13) + CHAR(10) +
    '    DROP VIEW [' + SCHEMA_NAME(v.schema_id) + '].[' + v.name + '];' + CHAR(13) + CHAR(10) +
    'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) +
    m.definition + CHAR(13) + CHAR(10) +
    'GO' AS [Script]
FROM sys.views v
INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
WHERE v.name = @ViewName;
*/

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================

-- Issue 1: View depends on objects that don't exist in target database
-- Solution: Ensure dependent tables/views are copied first, or manually create them

-- Issue 2: Cross-database references in views
-- Solution: Modify view definitions to use correct database names in target

-- Issue 3: Different schema names between source and target
-- Solution: Search and replace schema names in generated script before running

-- Issue 4: View uses features not supported in target SQL Server version
-- Solution: Manually modify view definitions for compatibility

-- ============================================================================
