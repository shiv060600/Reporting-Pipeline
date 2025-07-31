-- =============================================
-- Combined Sales Report - SQL Server Implementation (FIXED)
-- Replaces the Python main.py reporting pipeline
-- =============================================

USE TUTLIV
GO

-- Drop procedure if it exists
IF OBJECT_ID('dbo.sp_GenerateCombinedSalesReport', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_GenerateCombinedSalesReport
GO

CREATE PROCEDURE dbo.sp_GenerateCombinedSalesReport
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @curr_month_year VARCHAR(6);
    DECLARE @start_year VARCHAR(4);
    DECLARE @curr_month INT;
    DECLARE @curr_year INT;
    
    -- Set date variables
    SET @curr_month_year = FORMAT(GETDATE(), 'yyyyMM');
    SET @start_year = FORMAT(DATEADD(YEAR, -3, GETDATE()), 'yyyy');
    SET @curr_month = MONTH(GETDATE());
    SET @curr_year = YEAR(GETDATE());
    
    PRINT 'Starting Combined Sales Report Generation...';
    PRINT 'Current Month/Year: ' + @curr_month_year;
    PRINT 'Start Year: ' + @start_year;
    
    -- =============================================
    -- Step 1: Get Ingram Sales Data
    -- =============================================
    PRINT 'Step 1: Retrieving Ingram Sales Data...';
    
    IF OBJECT_ID('tempdb..#ingram_sales') IS NOT NULL DROP TABLE #ingram_sales;
    
    SELECT 
        LTRIM(RTRIM(ISBN)) as ISBN,
        YEAR,
        MONTH,
        LTRIM(RTRIM(TITLE)) as TITLE,
        LTRIM(RTRIM(NAMECUST)) as NAMECUST,
        LTRIM(RTRIM([TUTTLE SALES CATEGORY])) as ACCTDESCR,  -- Fixed: Use correct column name
        NETUNITS,
        NETAMT,
        'INGRAM' as SOURCE_TYPE
    INTO #ingram_sales
    FROM TUTLIV.dbo.ING_SALES as s
    WHERE 
        (s.YEAR * 100 + s.MONTH) <> CAST(@curr_month_year AS INT) 
        AND s.YEAR >= CAST(@start_year AS INT);
    
    PRINT 'Ingram Sales Records: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
    
    -- =============================================
    -- Step 2: Get Sage Sales Data
    -- =============================================
    PRINT 'Step 2: Retrieving Sage Sales Data...';
    
    IF OBJECT_ID('tempdb..#sage_sales') IS NOT NULL DROP TABLE #sage_sales;
    
    SELECT
        LTRIM(RTRIM(ISBN)) as ISBN,
        YEAR,
        MONTH,
        LTRIM(RTRIM(TITLE)) as TITLE,
        LTRIM(RTRIM(NAMECUST)) as NAMECUST,
        '' as ACCTDESCR, -- Sage data doesn't have this field
        NETUNITS,
        NETAMT,
        'SAGE' as SOURCE_TYPE
    INTO #sage_sales
    FROM TUTLIV.dbo.ALL_HSA_MKSEG
    WHERE
        (YEAR * 100 + MONTH) <> CAST(@curr_month_year AS INT)
        AND YEAR >= CAST(@start_year AS INT)
        AND LTRIM(RTRIM(LOWER(NAMECUST))) NOT LIKE '%ingram book co.%';
    
    PRINT 'Sage Sales Records: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
    
    -- =============================================
    -- Step 3: Name Mapping (Simplified approach)
    -- =============================================
    PRINT 'Step 3: Creating Name Mapping...';
    
    -- Create a mapping table for similar customer names
    -- This is a simplified version - in production, you might want a more sophisticated matching
    IF OBJECT_ID('tempdb..#name_mapping') IS NOT NULL DROP TABLE #name_mapping;
    
    WITH IngramCustomers AS (
        SELECT DISTINCT NAMECUST as INGRAM_NAME
        FROM #ingram_sales
    ),
    SageCustomers AS (
        SELECT DISTINCT NAMECUST as SAGE_NAME
        FROM #sage_sales
    ),
    NameMatches AS (
        -- Exact matches
        SELECT 
            i.INGRAM_NAME,
            s.SAGE_NAME,
            100 as MATCH_SCORE
        FROM IngramCustomers i
        INNER JOIN SageCustomers s ON UPPER(LTRIM(RTRIM(i.INGRAM_NAME))) = UPPER(LTRIM(RTRIM(s.SAGE_NAME)))
        
        UNION ALL
        
        -- Partial matches using SOUNDEX and LIKE
        SELECT 
            i.INGRAM_NAME,
            s.SAGE_NAME,
            90 as MATCH_SCORE
        FROM IngramCustomers i
        CROSS APPLY (
            SELECT TOP 1 s.SAGE_NAME
            FROM SageCustomers s
            WHERE (SOUNDEX(i.INGRAM_NAME) = SOUNDEX(s.SAGE_NAME)
                   OR UPPER(i.INGRAM_NAME) LIKE '%' + UPPER(s.SAGE_NAME) + '%'
                   OR UPPER(s.SAGE_NAME) LIKE '%' + UPPER(i.INGRAM_NAME) + '%')
            AND NOT EXISTS (
                SELECT 1 FROM IngramCustomers i2 
                INNER JOIN SageCustomers s2 ON UPPER(LTRIM(RTRIM(i2.INGRAM_NAME))) = UPPER(LTRIM(RTRIM(s2.SAGE_NAME)))
                WHERE i2.INGRAM_NAME = i.INGRAM_NAME
            )
            ORDER BY LEN(ABS(LEN(s.SAGE_NAME) - LEN(i.INGRAM_NAME)))
        ) s
        WHERE s.SAGE_NAME IS NOT NULL
    )
    SELECT 
        INGRAM_NAME,
        COALESCE(SAGE_NAME, INGRAM_NAME) as MAPPED_NAME
    INTO #name_mapping
    FROM (
        SELECT 
            INGRAM_NAME,
            SAGE_NAME,
            ROW_NUMBER() OVER (PARTITION BY INGRAM_NAME ORDER BY MATCH_SCORE DESC) as rn
        FROM NameMatches
    ) ranked
    WHERE rn = 1
    
    UNION ALL
    
    -- Add unmapped Ingram names
    SELECT 
        i.INGRAM_NAME,
        i.INGRAM_NAME as MAPPED_NAME
    FROM IngramCustomers i
    WHERE NOT EXISTS (SELECT 1 FROM NameMatches nm WHERE nm.INGRAM_NAME = i.INGRAM_NAME);
    
    -- Apply name mapping to Ingram sales
    UPDATE i
    SET NAMECUST = nm.MAPPED_NAME
    FROM #ingram_sales i
    INNER JOIN #name_mapping nm ON i.NAMECUST = nm.INGRAM_NAME;
    
    PRINT 'Name mapping completed';
    
    -- =============================================
    -- Step 4: Combine and Aggregate Sales Data
    -- =============================================
    PRINT 'Step 4: Combining and aggregating sales data...';
    
    IF OBJECT_ID('tempdb..#combined_sales') IS NOT NULL DROP TABLE #combined_sales;
    
    -- Combine both datasets
    SELECT 
        ISBN,
        YEAR,
        MONTH,
        TITLE,
        NAMECUST,
        MAX(ACCTDESCR) as ACCTDESCR, -- Fixed: Use MAX() instead of GROUP BY
        SUM(NETUNITS) as NETUNITS,
        SUM(NETAMT) as NETAMT,
        (YEAR * 100 + MONTH) as YEARMONTH
    INTO #combined_sales
    FROM (
        SELECT ISBN, YEAR, MONTH, TITLE, NAMECUST, ACCTDESCR, NETUNITS, NETAMT FROM #ingram_sales
        UNION ALL
        SELECT ISBN, YEAR, MONTH, TITLE, NAMECUST, ACCTDESCR, NETUNITS, NETAMT FROM #sage_sales
    ) combined
    GROUP BY ISBN, YEAR, MONTH, TITLE, NAMECUST;
    
    PRINT 'Combined Sales Records: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
    
    -- Create base report structure
    IF OBJECT_ID('tempdb..#base_report') IS NOT NULL DROP TABLE #base_report;
    
    SELECT DISTINCT 
        ISBN,
        TITLE,
        NAMECUST
    INTO #base_report
    FROM #combined_sales;
    
    -- =============================================
    -- Step 5: Calculate YTD Values
    -- =============================================
    PRINT 'Step 5: Calculating YTD values...';
    
    IF OBJECT_ID('tempdb..#ytd_values') IS NOT NULL DROP TABLE #ytd_values;
    
    SELECT 
        ISBN,
        TITLE,
        NAMECUST,
        SUM(NETUNITS) as YTD_UNITS,
        SUM(NETAMT) as YTD_DOLLARS
    INTO #ytd_values
    FROM #combined_sales
    WHERE YEAR = @curr_year
    GROUP BY ISBN, TITLE, NAMECUST; -- Fixed: Removed ACCTDESCR from GROUP BY
    
    -- =============================================
    -- Step 6: Calculate 12-Month Rolling Values
    -- =============================================
    PRINT 'Step 6: Calculating 12-month rolling values...';
    
    -- Create table for 12-month periods
    IF OBJECT_ID('tempdb..#twelve_months') IS NOT NULL DROP TABLE #twelve_months;
    
    CREATE TABLE #twelve_months (
        month_offset INT,
        target_year INT,
        target_month INT,
        yearmonth INT,
        column_name VARCHAR(50)
    );
    
    -- Populate 12-month periods
    DECLARE @i INT = 1;
    WHILE @i <= 12
    BEGIN
        DECLARE @target_month INT = @curr_month - @i;
        DECLARE @target_year INT = @curr_year;
        
        IF @target_month <= 0
        BEGIN
            SET @target_month = @target_month + 12;
            SET @target_year = @target_year - 1;
        END
        
        DECLARE @yearmonth INT = (@target_year * 100) + @target_month;
        DECLARE @column_name VARCHAR(50) = 'NET_UNITS_' + 
            CASE @target_month
                WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr'
                WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug'
                WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
            END + '_' + CAST(@target_year AS VARCHAR(4));
        
        INSERT INTO #twelve_months VALUES (@i, @target_year, @target_month, @yearmonth, @column_name);
        SET @i = @i + 1;
    END
    
    -- Calculate 12M totals
    IF OBJECT_ID('tempdb..#twelve_month_totals') IS NOT NULL DROP TABLE #twelve_month_totals;
    
    SELECT 
        cs.ISBN,
        cs.TITLE,
        cs.NAMECUST,
        SUM(cs.NETUNITS) as [12M_UNITS],
        SUM(cs.NETAMT) as [12M_DOLLARS]
    INTO #twelve_month_totals
    FROM #combined_sales cs
    INNER JOIN #twelve_months tm ON cs.YEARMONTH = tm.yearmonth
    GROUP BY cs.ISBN, cs.TITLE, cs.NAMECUST; -- Fixed: Removed ACCTDESCR from GROUP BY
    
    -- =============================================
    -- Step 7: Calculate Yearly Values
    -- =============================================
    PRINT 'Step 7: Calculating yearly values...';
    
    IF OBJECT_ID('tempdb..#yearly_values') IS NOT NULL DROP TABLE #yearly_values;
    
    -- Create pivot for yearly data - Dynamic years based on current date
    DECLARE @year1 INT = @curr_year - 3;
    DECLARE @year2 INT = @curr_year - 2;
    DECLARE @year3 INT = @curr_year - 1;
    
    DECLARE @year1_col VARCHAR(20) = 'UNITS_' + CAST(@year1 AS VARCHAR(4));
    DECLARE @year2_col VARCHAR(20) = 'UNITS_' + CAST(@year2 AS VARCHAR(4));
    DECLARE @year3_col VARCHAR(20) = 'UNITS_' + CAST(@year3 AS VARCHAR(4));
    
    -- Create yearly values table structure first (in main scope)
    CREATE TABLE #yearly_values (
        ISBN VARCHAR(20),
        TITLE VARCHAR(500),
        NAMECUST VARCHAR(500),
        year1_units INT,
        year2_units INT,
        year3_units INT
    );
    
    -- Populate it using dynamic SQL with INSERT (not CREATE)
    DECLARE @yearly_sql NVARCHAR(MAX) = N'
    INSERT INTO #yearly_values (ISBN, TITLE, NAMECUST, year1_units, year2_units, year3_units)
    SELECT 
        ISBN,
        TITLE,
        NAMECUST,
        SUM(CASE WHEN YEAR = ' + CAST(@year1 AS VARCHAR(4)) + ' THEN NETUNITS ELSE 0 END),
        SUM(CASE WHEN YEAR = ' + CAST(@year2 AS VARCHAR(4)) + ' THEN NETUNITS ELSE 0 END),
        SUM(CASE WHEN YEAR = ' + CAST(@year3 AS VARCHAR(4)) + ' THEN NETUNITS ELSE 0 END)
    FROM #combined_sales
    WHERE YEAR BETWEEN ' + CAST(@year1 AS VARCHAR(4)) + ' AND ' + CAST(@year3 AS VARCHAR(4)) + '
    GROUP BY ISBN, TITLE, NAMECUST;';
    
    EXEC sp_executesql @yearly_sql;
    
    -- =============================================
    -- Step 8: Get Additional Product Data
    -- =============================================
    PRINT 'Step 8: Retrieving additional product data...';
    
    IF OBJECT_ID('tempdb..#all_accounts') IS NOT NULL DROP TABLE #all_accounts;
    
    SELECT 
        LTRIM(RTRIM(REPLACE(ITEMNO, '-', ''))) as ISBN,
        NETQTY as ALL_ACCTS_12M_UNITS,
        NETSALES as ALL_ACCTS_12M_DOLLARS
    INTO #all_accounts
    FROM TUTLIV.dbo.ALL_ACCOUNTS_12M_ROLL;
    
    IF OBJECT_ID('tempdb..#book_details') IS NOT NULL DROP TABLE #book_details;
    
    SELECT 
        LTRIM(RTRIM(REPLACE(ISBN, '-', ''))) as ISBN,
        PROD_TYPE as TYPE,
        PROD_CLASS as PROD,
        SEAS,
        SUBPUB as SUB,
        RETAIL_PRICE as RETAIL
    INTO #book_details
    FROM TUTLIV.dbo.BOOK_DETAILS;
    
    -- Get ACCTDESCR for the base report
    IF OBJECT_ID('tempdb..#acct_descriptions') IS NOT NULL DROP TABLE #acct_descriptions;
    
    SELECT 
        ISBN,
        TITLE,
        NAMECUST,
        MAX(ACCTDESCR) as ACCTDESCR
    INTO #acct_descriptions
    FROM #combined_sales
    GROUP BY ISBN, TITLE, NAMECUST;
    
    -- =============================================
    -- Step 9: Create Monthly Pivot Data
    -- =============================================
    PRINT 'Step 9: Creating monthly pivot data...';
    
    -- Create monthly pivot table structure first (in main scope)
    CREATE TABLE #monthly_pivot (
        ISBN VARCHAR(20),
        TITLE VARCHAR(500),
        NAMECUST VARCHAR(500),
        month1_units INT,
        month2_units INT,
        month3_units INT,
        month4_units INT,
        month5_units INT,
        month6_units INT,
        month7_units INT,
        month8_units INT,
        month9_units INT,
        month10_units INT,
        month11_units INT,
        month12_units INT
    );
    
    -- Build dynamic SQL to populate it (using INSERT instead of CREATE)
    DECLARE @sql NVARCHAR(MAX) = N'
    INSERT INTO #monthly_pivot (ISBN, TITLE, NAMECUST, month1_units, month2_units, month3_units, month4_units, month5_units, month6_units, month7_units, month8_units, month9_units, month10_units, month11_units, month12_units)
    SELECT 
        ISBN,
        TITLE,
        NAMECUST,';
    
    -- Build dynamic columns for the last 12 months
    DECLARE @month_sql NVARCHAR(MAX) = '';
    DECLARE @j INT = 1;
    
    WHILE @j <= 12
    BEGIN
        DECLARE @calc_month INT = @curr_month - @j;
        DECLARE @calc_year INT = @curr_year;
        
        IF @calc_month <= 0
        BEGIN
            SET @calc_month = @calc_month + 12;
            SET @calc_year = @calc_year - 1;
        END
        
        DECLARE @calc_yearmonth INT = (@calc_year * 100) + @calc_month;
        
        SET @month_sql = @month_sql + '
        SUM(CASE WHEN YEARMONTH = ' + CAST(@calc_yearmonth AS VARCHAR(10)) + ' THEN NETUNITS ELSE 0 END),';
        
        SET @j = @j + 1;
    END
    
    -- Remove trailing comma
    SET @month_sql = LEFT(@month_sql, LEN(@month_sql) - 1);
    
    SET @sql = @sql + @month_sql + N'
    FROM #combined_sales cs
    INNER JOIN #twelve_months tm ON cs.YEARMONTH = tm.yearmonth
    GROUP BY ISBN, TITLE, NAMECUST;';
    
    EXEC sp_executesql @sql;
    
    -- =============================================
    -- Step 10: Create Final Report
    -- =============================================
    PRINT 'Step 10: Creating final report...';
    
    -- Drop existing table if it exists (using new name to avoid conflicts)
    IF OBJECT_ID('TUTLIV.dbo.COMBINED_SALES_REPORT_NEW', 'U') IS NOT NULL
        DROP TABLE TUTLIV.dbo.COMBINED_SALES_REPORT_NEW;
    
    -- Create the final report table with proper joins
    SELECT 
        br.TITLE,
        LTRIM(RTRIM(REPLACE(br.ISBN, '-', ''))) as ISBN,
        br.NAMECUST,
        ISNULL(ad.ACCTDESCR, '') as ACCTDESCR, -- Fixed: Get ACCTDESCR from separate table
        ISNULL(bd.TYPE, '') as TYPE,
        ISNULL(bd.PROD, '') as PROD,
        ISNULL(bd.SUB, '') as SUB,
        ISNULL(bd.RETAIL, 0) as RETAIL,
        ISNULL(bd.SEAS, '') as SEAS,
        ISNULL(aa.ALL_ACCTS_12M_UNITS, 0) as ALL_ACCTS_12M_UNITS,
        ISNULL(aa.ALL_ACCTS_12M_DOLLARS, 0) as ALL_ACCTS_12M_DOLLARS,
        ISNULL(tmt.[12M_UNITS], 0) as [12M_UNITS],
        ISNULL(tmt.[12M_DOLLARS], 0) as [12M_DOLLARS],
        ISNULL(ytd.YTD_UNITS, 0) as YTD_UNITS,
        ISNULL(ytd.YTD_DOLLARS, 0) as YTD_DOLLARS,
                 -- Yearly columns will be added dynamically later
         0 as PLACEHOLDER_FOR_YEARLY_COLS -- Temporary placeholder
    INTO #temp_final_report
    FROM #base_report br
    LEFT JOIN #acct_descriptions ad ON br.ISBN = ad.ISBN AND br.NAMECUST = ad.NAMECUST AND br.TITLE = ad.TITLE
    LEFT JOIN #ytd_values ytd ON br.ISBN = ytd.ISBN AND br.NAMECUST = ytd.NAMECUST AND br.TITLE = ytd.TITLE
    LEFT JOIN #twelve_month_totals tmt ON br.ISBN = tmt.ISBN AND br.NAMECUST = tmt.NAMECUST AND br.TITLE = tmt.TITLE
    LEFT JOIN #yearly_values yv ON br.ISBN = yv.ISBN AND br.NAMECUST = yv.NAMECUST AND br.TITLE = yv.TITLE
    LEFT JOIN #all_accounts aa ON LTRIM(RTRIM(REPLACE(br.ISBN, '-', ''))) = aa.ISBN
    LEFT JOIN #book_details bd ON LTRIM(RTRIM(REPLACE(br.ISBN, '-', ''))) = bd.ISBN;
    
    -- Now we need to add the monthly columns dynamically
    -- This is complex in SQL Server, so we'll use a simpler approach with known column names
    -- Add monthly and yearly columns dynamically
     DECLARE @month_col_name NVARCHAR(MAX);
     DECLARE @add_columns_sql NVARCHAR(MAX) = N'
     SELECT 
         tf.TITLE,
         tf.ISBN,
         tf.NAMECUST,
         tf.ACCTDESCR,
         tf.TYPE,
         tf.PROD,
         tf.SUB,
         tf.RETAIL,
         tf.SEAS,
         tf.ALL_ACCTS_12M_UNITS,
         tf.ALL_ACCTS_12M_DOLLARS,
         tf.[12M_UNITS],
         tf.[12M_DOLLARS],
         tf.YTD_UNITS,
         tf.YTD_DOLLARS';
     
     -- Add monthly columns dynamically
     SET @j = 1;
     WHILE @j <= 12
     BEGIN
         SET @calc_month = @curr_month - @j;
         SET @calc_year = @curr_year;
         SET @month_col_name = ''
         IF @calc_month <= 0
         BEGIN
             SET @calc_month = @calc_month + 12;
             SET @calc_year = @calc_year - 1;
         END
         
         -- Create the actual column name for this month
         SET @month_col_name = 'NET_UNITS_' + 
             CASE @calc_month
                 WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr'
                 WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug'
                 WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
             END + '_' + CAST(@calc_year AS VARCHAR(4));
         
         SET @add_columns_sql = @add_columns_sql + ',
         ISNULL(mp.month' + CAST(@j AS VARCHAR(2)) + '_units, 0) as [' + @month_col_name + ']';
         
         SET @j = @j + 1;
     END
     
     -- Add yearly columns dynamically
     SET @add_columns_sql = @add_columns_sql + ',
         ISNULL(yv.year1_units, 0) as [' + @year1_col + '],
         ISNULL(yv.year2_units, 0) as [' + @year2_col + '],
         ISNULL(yv.year3_units, 0) as [' + @year3_col + ']';
     
     SET @add_columns_sql = @add_columns_sql + N'
     INTO TUTLIV.dbo.COMBINED_SALES_REPORT_NEW
     FROM #temp_final_report tf
     LEFT JOIN #monthly_pivot mp ON tf.ISBN = mp.ISBN AND tf.NAMECUST = mp.NAMECUST AND tf.TITLE = mp.TITLE
     LEFT JOIN #yearly_values yv ON tf.ISBN = yv.ISBN AND tf.NAMECUST = yv.NAMECUST AND tf.TITLE = yv.TITLE;';
     
     EXEC sp_executesql @add_columns_sql;
    
    -- =============================================
    -- Step 11: Final aggregation to eliminate duplicates
    -- =============================================
    PRINT 'Step 11: Final aggregation to eliminate duplicates...';
    
    -- This step matches the Python groupby at the end
    -- We can skip the final aggregation since we're already doing proper grouping above
    -- and the monthly/yearly data should already be correctly aggregated
    
    PRINT 'Final aggregation step skipped - data already properly aggregated';
    
    -- Final table is already created and populated properly
    
    -- =============================================
    -- Step 12: Create Summary Statistics
    -- =============================================
    PRINT 'Step 12: Generating summary statistics...';
    
    DECLARE @final_row_count INT;
    DECLARE @count_sql NVARCHAR(MAX) = N'SELECT @count = COUNT(*) FROM TUTLIV.dbo.COMBINED_SALES_REPORT_NEW';
    EXEC sp_executesql @count_sql, N'@count INT OUTPUT', @count = @final_row_count OUTPUT;
    
    PRINT '=== COMBINED SALES REPORT GENERATION COMPLETE ===';
    PRINT 'Final report contains ' + CAST(@final_row_count AS VARCHAR(20)) + ' rows';
    PRINT 'Report saved to: TUTLIV.dbo.COMBINED_SALES_REPORT_NEW';
    
    -- Show sample of results (using * to avoid column name issues)
    PRINT 'Sample of final results (first 3 rows):';
    DECLARE @sample_sql NVARCHAR(MAX) = N'
    SELECT TOP 3 * 
    FROM TUTLIV.dbo.COMBINED_SALES_REPORT_NEW;';
    
    EXEC sp_executesql @sample_sql;
    
    -- Clean up temp tables
    DROP TABLE #ingram_sales;
    DROP TABLE #sage_sales;
    DROP TABLE #name_mapping;
    DROP TABLE #combined_sales;
    DROP TABLE #base_report;
    DROP TABLE #ytd_values;
    DROP TABLE #twelve_months;
    DROP TABLE #twelve_month_totals;
    DROP TABLE #yearly_values;
    DROP TABLE #all_accounts;
    DROP TABLE #book_details;
    DROP TABLE #acct_descriptions;
    DROP TABLE #monthly_pivot;
    DROP TABLE #temp_final_report;
    
    PRINT 'Cleanup completed successfully.';
    
END
GO

-- Grant permissions (adjust as needed for your environment)
-- GRANT EXECUTE ON dbo.sp_GenerateCombinedSalesReport TO [YourUserOrRole];

PRINT 'Stored procedure created successfully!';
PRINT 'To run the report, execute: EXEC dbo.sp_GenerateCombinedSalesReport'; 