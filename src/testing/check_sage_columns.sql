-- =============================================
-- CHECK ALL_HSA_MKSEG TABLE STRUCTURE
-- =============================================

USE TUTLIV
GO

-- Check the actual column names in ALL_HSA_MKSEG
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ALL_HSA_MKSEG'
ORDER BY ORDINAL_POSITION

-- Check a sample of the data to see the actual structure
SELECT TOP 10 *
FROM TUTLIV.dbo.ALL_HSA_MKSEG

-- Check for customer/bill-to related columns
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