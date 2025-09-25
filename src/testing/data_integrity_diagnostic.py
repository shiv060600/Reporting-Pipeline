#type: ignore
import pandas as pd
import numpy as np
import sqlalchemy
import urllib
import polars as pl
import logging
import sys
import os

# Add the helpers path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from helpers.paths import PATHS

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs_and_tests/data_integrity_diagnostic.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

SSMS_CONN_STRING = PATHS["SSMS_CONN_STRING"]
params = urllib.parse.quote_plus(SSMS_CONN_STRING)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

def diagnose_data_loss():
    """
    Diagnostic script to identify the exact source of data loss in the pipeline.
    """
    logger.info("=" * 80)
    logger.info("DATA INTEGRITY DIAGNOSTIC - IDENTIFYING DATA LOSS SOURCES")
    logger.info("=" * 80)
    
    # Step 1: Get original data
    logger.info("Step 1: Retrieving original Ingram and Sage sales data...")
    
    ingram_sales_df = pl.from_pandas(pd.read_sql(
        """
        DECLARE @curr_month_year VARCHAR(6);
        DECLARE @start_year VARCHAR(4);

        SET @curr_month_year = FORMAT(GETDATE(),'yyyyMM')
        SET @start_year = FORMAT(DATEADD(YEAR,-3,GETDATE()),'yyyy')

        SELECT 
            TRIM(ING_SALES.ISBN) as ISBN,
            ING_SALES.YEAR,
            ING_SALES.MONTH,
            TRIM(ING_SALES.TITLE) as TITLE,
            COALESCE(TRIM(NAME_MAP.MAPPED_SAGE_NAME), TRIM(ING_SALES.NAMECUST)) as NAMECUST,
            ING_SALES.NETUNITS,
            ING_SALES.NETAMT,
            TRIM(ING_CAT.[MASTER SALES CATEGORY]) as TUTTLE_SALES_CATEGORY
        FROM
            TUTLIV.dbo.ING_SALES as ING_SALES 
            LEFT JOIN (
                SELECT DISTINCT 
                    TRIM([SL Account Number]) as [SL Account Number],
                    TRIM([HQ Account Number]) as [HQ Account Number],
                    TRIM([MASTER SALES CATEGORY]) as [MASTER SALES CATEGORY]
                FROM TUTLIV.dbo.INGRAM_MASTER_CATEGORIES
            ) ING_CAT
                ON TRIM(ING_SALES.[SL Account Number]) = ING_CAT.[SL Account Number]
                AND TRIM(ING_SALES.[HQ Account Number]) = ING_CAT.[HQ Account Number]
            LEFT JOIN (
                SELECT DISTINCT 
                    TRIM(INGRAM_NAME) as INGRAM_NAME,
                    TRIM(MAPPED_SAGE_NAME) as MAPPED_SAGE_NAME
                FROM TUTLIV.dbo.MASTER_INGRAM_NAME_MAPPING
            ) NAME_MAP 
                ON TRIM(ING_SALES.NAMECUST) = NAME_MAP.INGRAM_NAME
        WHERE 
            (ING_SALES.YEAR * 100 + ING_SALES.MONTH) <> @curr_month_year AND
            ING_SALES.YEAR >= @start_year
        """, engine))

    sage_sales_df = pl.from_pandas(pd.read_sql(
        """
        DECLARE @curr_month_year Varchar(6);
        DECLARE @start_year Varchar(4);

        SET @curr_month_year = FORMAT(GETDATE(),'yyyyMM')
        SET @start_year = FORMAT(DATEADD(YEAR,-3,GETDATE()),'yyyy')

        SELECT
            TRIM(SAGE_HSA.ISBN) as ISBN,
            SAGE_HSA.YEAR,
            SAGE_HSA.MONTH,
            TRIM(SAGE_HSA.TITLE) as TITLE,
            TRIM(SAGE_HSA.NAMECUST) as NAMECUST,
            SAGE_HSA.NETUNITS,
            SAGE_HSA.NETAMT,
            TRIM(SAGE_CAT.TUTTLE_SALES_CATEGORY) as TUTTLE_SALES_CATEGORY
        FROM
            TUTLIV.dbo.ALL_HSA_MKSEG as SAGE_HSA
            LEFT JOIN (
                SELECT DISTINCT 
                    TRIM(IDCUST) as IDCUST,
                    TRIM(TUTTLE_SALES_CATEGORY) as TUTTLE_SALES_CATEGORY
                FROM TUTLIV.dbo.SAGE_MASTER_CATEGORIES
            ) SAGE_CAT
                ON TRIM(SAGE_CAT.IDCUST) = TRIM(SAGE_HSA.NEWBILLTO)
        WHERE
            (SAGE_HSA.YEAR * 100 + SAGE_HSA.MONTH) <> @curr_month_year AND
            SAGE_HSA.YEAR >= @start_year AND
            TRIM(SAGE_HSA.NAMECUST) NOT LIKE '%ingram book co.%'
        """ , engine))
    
    logger.info(f"Ingram sales: {len(ingram_sales_df):,} records")
    logger.info(f"Sage sales: {len(sage_sales_df):,} records")
    
    # Step 2: Check for null TUTTLE_SALES_CATEGORY
    logger.info("\nStep 2: Checking for null TUTTLE_SALES_CATEGORY values...")
    
    ingram_null_cat = ingram_sales_df.filter(pl.col('TUTTLE_SALES_CATEGORY').is_null()).height
    sage_null_cat = sage_sales_df.filter(pl.col('TUTTLE_SALES_CATEGORY').is_null()).height
    
    logger.info(f"Ingram records with null TUTTLE_SALES_CATEGORY: {ingram_null_cat:,}")
    logger.info(f"Sage records with null TUTTLE_SALES_CATEGORY: {sage_null_cat:,}")
    
    if ingram_null_cat > 0 or sage_null_cat > 0:
        logger.warning("WARNING: Null TUTTLE_SALES_CATEGORY values found!")
        logger.warning("This could cause data loss during grouping operations.")
    
    # Step 3: Check unique combinations
    logger.info("\nStep 3: Analyzing unique combinations...")
    
    ingram_unique = ingram_sales_df.select(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY']).unique()
    sage_unique = sage_sales_df.select(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY']).unique()
    
    logger.info(f"Ingram unique combinations: {len(ingram_unique):,}")
    logger.info(f"Sage unique combinations: {len(sage_unique):,}")
    
    # Step 4: Check for duplicates in unique combinations
    logger.info("\nStep 4: Checking for duplicates in unique combinations...")
    
    ingram_duplicates = ingram_unique.group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY']).count().filter(pl.col('count') > 1)
    sage_duplicates = sage_unique.group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY']).count().filter(pl.col('count') > 1)
    
    logger.info(f"Ingram duplicate combinations: {len(ingram_duplicates):,}")
    logger.info(f"Sage duplicate combinations: {len(sage_duplicates):,}")
    
    if len(ingram_duplicates) > 0 or len(sage_duplicates) > 0:
        logger.warning("WARNING: Duplicate combinations found!")
        logger.warning("This could cause data loss during grouping operations.")
    
    # Step 5: Check for empty strings in key fields
    logger.info("\nStep 5: Checking for empty strings in key fields...")
    
    ingram_empty_isbn = ingram_sales_df.filter(pl.col('ISBN').str.strip_chars() == '').height
    ingram_empty_title = ingram_sales_df.filter(pl.col('TITLE').str.strip_chars() == '').height
    ingram_empty_name = ingram_sales_df.filter(pl.col('NAMECUST').str.strip_chars() == '').height
    
    sage_empty_isbn = sage_sales_df.filter(pl.col('ISBN').str.strip_chars() == '').height
    sage_empty_title = sage_sales_df.filter(pl.col('TITLE').str.strip_chars() == '').height
    sage_empty_name = sage_sales_df.filter(pl.col('NAMECUST').str.strip_chars() == '').height
    
    logger.info(f"Ingram empty ISBN: {ingram_empty_isbn:,}")
    logger.info(f"Ingram empty TITLE: {ingram_empty_title:,}")
    logger.info(f"Ingram empty NAMECUST: {ingram_empty_name:,}")
    logger.info(f"Sage empty ISBN: {sage_empty_isbn:,}")
    logger.info(f"Sage empty TITLE: {sage_empty_title:,}")
    logger.info(f"Sage empty NAMECUST: {sage_empty_name:,}")
    
    # Step 6: Check for very long strings that might cause issues
    logger.info("\nStep 6: Checking for very long strings...")
    
    ingram_long_title = ingram_sales_df.filter(pl.col('TITLE').str.len_chars() > 500).height
    sage_long_title = sage_sales_df.filter(pl.col('TITLE').str.len_chars() > 500).height
    
    logger.info(f"Ingram titles > 500 chars: {ingram_long_title:,}")
    logger.info(f"Sage titles > 500 chars: {sage_long_title:,}")
    
    # Step 7: Check for special characters that might cause issues
    logger.info("\nStep 7: Checking for special characters...")
    
    ingram_special_chars = ingram_sales_df.filter(
        pl.col('TITLE').str.contains(r'[^\w\s\-\.]') | 
        pl.col('NAMECUST').str.contains(r'[^\w\s\-\.]')
    ).height
    
    sage_special_chars = sage_sales_df.filter(
        pl.col('TITLE').str.contains(r'[^\w\s\-\.]') | 
        pl.col('NAMECUST').str.contains(r'[^\w\s\-\.]')
    ).height
    
    logger.info(f"Ingram records with special chars: {ingram_special_chars:,}")
    logger.info(f"Sage records with special chars: {sage_special_chars:,}")
    
    # Step 8: Check for data type issues
    logger.info("\nStep 8: Checking data types...")
    
    logger.info(f"Ingram schema: {ingram_sales_df.schema}")
    logger.info(f"Sage schema: {sage_sales_df.schema}")
    
    # Step 9: Check for extreme values
    logger.info("\nStep 9: Checking for extreme values...")
    
    ingram_zero_units = ingram_sales_df.filter(pl.col('NETUNITS') == 0).height
    ingram_zero_amt = ingram_sales_df.filter(pl.col('NETAMT') == 0).height
    sage_zero_units = sage_sales_df.filter(pl.col('NETUNITS') == 0).height
    sage_zero_amt = sage_sales_df.filter(pl.col('NETAMT') == 0).height
    
    logger.info(f"Ingram zero units: {ingram_zero_units:,}")
    logger.info(f"Ingram zero amount: {ingram_zero_amt:,}")
    logger.info(f"Sage zero units: {sage_zero_units:,}")
    logger.info(f"Sage zero amount: {sage_zero_amt:,}")
    
    # Step 10: Summary and recommendations
    logger.info("\n" + "=" * 80)
    logger.info("DIAGNOSTIC SUMMARY AND RECOMMENDATIONS")
    logger.info("=" * 80)
    
    total_issues = 0
    
    if ingram_null_cat > 0 or sage_null_cat > 0:
        logger.error("ISSUE: Null TUTTLE_SALES_CATEGORY values found")
        logger.error("RECOMMENDATION: Fill null values with a default category or filter them out")
        total_issues += 1
    
    if len(ingram_duplicates) > 0 or len(sage_duplicates) > 0:
        logger.error("ISSUE: Duplicate combinations found")
        logger.error("RECOMMENDATION: Investigate why duplicates exist and handle them appropriately")
        total_issues += 1
    
    if ingram_empty_isbn > 0 or sage_empty_isbn > 0:
        logger.error("ISSUE: Empty ISBN values found")
        logger.error("RECOMMENDATION: Filter out records with empty ISBNs")
        total_issues += 1
    
    if ingram_empty_title > 0 or sage_empty_title > 0:
        logger.error("ISSUE: Empty TITLE values found")
        logger.error("RECOMMENDATION: Fill empty titles or filter them out")
        total_issues += 1
    
    if ingram_empty_name > 0 or sage_empty_name > 0:
        logger.error("ISSUE: Empty NAMECUST values found")
        logger.error("RECOMMENDATION: Fill empty customer names or filter them out")
        total_issues += 1
    
    if total_issues == 0:
        logger.info("SUCCESS: No obvious data quality issues found!")
        logger.info("The data loss might be due to join operations or grouping logic.")
    else:
        logger.warning(f"Found {total_issues} potential data quality issues")
        logger.warning("Address these issues before running the main pipeline")
    
    logger.info("=" * 80)

if __name__ == "__main__":
    diagnose_data_loss() 