-- =============================================
-- SAGE JOIN ANALYSIS - FIXED VERSION
-- =============================================

USE TUTLIV
GO

-- =============================================
-- STEP 1: Check ALL_HSA_MKSEG table structure
-- =============================================

PRINT '=== CHECKING ALL_HSA_MKSEG TABLE STRUCTURE ==='

-- Check all columns
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ALL_HSA_MKSEG'
ORDER BY ORDINAL_POSITION

-- Check for customer/bill-to related columns specifically
PRINT ''
PRINT '=== CUSTOMER/BILL-TO RELATED COLUMNS ==='

SELECT 
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ALL_HSA_MKSEG' 
AND (
    COLUMN_NAME LIKE '%CUST%' OR 
    COLUMN_NAME LIKE '%BILL%' OR 
    COLUMN_NAME LIKE '%ACCT%' OR
    COLUMN_NAME LIKE '%ID%'
)
ORDER BY COLUMN_NAME

-- =============================================
-- STEP 2: Sample data to understand structure
-- =============================================

PRINT ''
PRINT '=== SAMPLE DATA FROM ALL_HSA_MKSEG ==='

SELECT TOP 5 *
FROM TUTLIV.dbo.ALL_HSA_MKSEG

-- =============================================
-- STEP 3: Check SAGE_MASTER_CATEGORIES structure
-- =============================================

PRINT ''
PRINT '=== CHECKING SAGE_MASTER_CATEGORIES TABLE STRUCTURE ==='

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'SAGE_MASTER_CATEGORIES'
ORDER BY ORDINAL_POSITION

-- =============================================
-- STEP 4: Raw ALL_HSA_MKSEG data (no joins)
-- =============================================

DECLARE @curr_month_year VARCHAR(6);
DECLARE @start_year VARCHAR(4);

SET @curr_month_year = FORMAT(GETDATE(),'yyyyMM')
SET @start_year = FORMAT(DATEADD(YEAR,-3,GETDATE()),'yyyy')

PRINT ''
PRINT '=== RAW ALL_HSA_MKSEG DATA (NO JOINS) ==='
PRINT 'Current Month/Year: ' + @curr_month_year
PRINT 'Start Year: ' + @start_year

SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT ISBN) as unique_isbns,
    COUNT(DISTINCT NAMECUST) as unique_customers,
    SUM(NETUNITS) as total_units,
    SUM(NETAMT) as total_amount
FROM TUTLIV.dbo.ALL_HSA_MKSEG
WHERE 
    (YEAR * 100 + MONTH) <> CAST(@curr_month_year AS INT) AND
    YEAR >= CAST(@start_year AS INT) AND
    TRIM(NAMECUST) NOT LIKE '%ingram book co.%'

-- =============================================
-- STEP 5: Test with different possible join columns
-- =============================================

PRINT ''
PRINT '=== TESTING DIFFERENT JOIN COLUMNS ==='

-- Try with IDACCTSET (if it exists)
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ALL_HSA_MKSEG' AND COLUMN_NAME = 'IDACCTSET')
BEGIN
    PRINT 'Testing join with IDACCTSET...'
    
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN SAGE_CAT.TUTTLE_SALES_CATEGORY IS NULL THEN 1 END) as null_categories,
        COUNT(CASE WHEN SAGE_CAT.TUTTLE_SALES_CATEGORY IS NOT NULL THEN 1 END) as with_categories
    FROM TUTLIV.dbo.ALL_HSA_MKSEG as SAGE_HSA
    LEFT JOIN (
        SELECT DISTINCT 
            TRIM(IDCUST) as IDCUST,
            TRIM(TUTTLE_SALES_CATEGORY) as TUTTLE_SALES_CATEGORY
        FROM TUTLIV.dbo.SAGE_MASTER_CATEGORIES
    ) SAGE_CAT
        ON TRIM(SAGE_CAT.IDCUST) = TRIM(SAGE_HSA.IDACCTSET)
    WHERE
        (SAGE_HSA.YEAR * 100 + SAGE_HSA.MONTH) <> CAST(@curr_month_year AS INT) AND
        SAGE_HSA.YEAR >= CAST(@start_year AS INT) AND
        TRIM(SAGE_HSA.NAMECUST) NOT LIKE '%ingram book co.%'
END
ELSE
BEGIN
    PRINT 'IDACCTSET column does not exist'
END

-- Try with NAMECUST (if that's the join column)
PRINT ''
PRINT 'Testing join with NAMECUST...'

SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN SAGE_CAT.TUTTLE_SALES_CATEGORY IS NULL THEN 1 END) as null_categories,
    COUNT(CASE WHEN SAGE_CAT.TUTTLE_SALES_CATEGORY IS NOT NULL THEN 1 END) as with_categories
FROM TUTLIV.dbo.ALL_HSA_MKSEG as SAGE_HSA
LEFT JOIN (
    SELECT DISTINCT 
        TRIM(IDCUST) as IDCUST,
        TRIM(TUTTLE_SALES_CATEGORY) as TUTTLE_SALES_CATEGORY
    FROM TUTLIV.dbo.SAGE_MASTER_CATEGORIES
) SAGE_CAT
    ON TRIM(SAGE_CAT.IDCUST) = TRIM(SAGE_HSA.NAMECUST)
WHERE
    (SAGE_HSA.YEAR * 100 + SAGE_HSA.MONTH) <> CAST(@curr_month_year AS INT) AND
    SAGE_HSA.YEAR >= CAST(@start_year AS INT) AND
    TRIM(SAGE_HSA.NAMECUST) NOT LIKE '%ingram book co.%'

-- =============================================
-- STEP 6: Check what columns actually exist
-- =============================================

PRINT ''
PRINT '=== SUMMARY OF AVAILABLE COLUMNS ==='

SELECT 
    'ALL_HSA_MKSEG' as table_name,
    STRING_AGG(COLUMN_NAME, ', ') as columns
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ALL_HSA_MKSEG'

UNION ALL

SELECT 
    'SAGE_MASTER_CATEGORIES' as table_name,
    STRING_AGG(COLUMN_NAME, ', ') as columns
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'SAGE_MASTER_CATEGORIES'

PRINT ''
PRINT '=== ANALYSIS COMPLETE ==='
PRINT 'Please check the column names above and update the join condition accordingly.' 