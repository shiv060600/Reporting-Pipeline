#type: ignore
import pandas as pd
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
        logging.FileHandler("fix_sage_categories.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

SSMS_CONN_STRING = PATHS["SSMS_CONN_STRING"]
params = urllib.parse.quote_plus(SSMS_CONN_STRING)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

def investigate_sage_categories():
    """
    Investigate the missing TUTTLE_SALES_CATEGORY values in Sage data.
    """
    logger.info("=" * 80)
    logger.info("INVESTIGATING MISSING SAGE TUTTLE_SALES_CATEGORY VALUES")
    logger.info("=" * 80)
    
    # Check the SAGE_MASTER_CATEGORIES table
    logger.info("Step 1: Checking SAGE_MASTER_CATEGORIES table...")
    
    sage_categories_df = pl.from_pandas(pd.read_sql(
        """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT IDCUST) as unique_customers,
            COUNT(DISTINCT TUTTLE_SALES_CATEGORY) as unique_categories,
            COUNT(CASE WHEN TUTTLE_SALES_CATEGORY IS NULL OR TRIM(TUTTLE_SALES_CATEGORY) = '' THEN 1 END) as null_categories
        FROM TUTLIV.dbo.SAGE_MASTER_CATEGORIES
        """,
        engine
    ))
    
    logger.info(f"SAGE_MASTER_CATEGORIES summary:")
    logger.info(f"  Total records: {sage_categories_df['total_records'][0]:,}")
    logger.info(f"  Unique customers: {sage_categories_df['unique_customers'][0]:,}")
    logger.info(f"  Unique categories: {sage_categories_df['unique_categories'][0]:,}")
    logger.info(f"  Null categories: {sage_categories_df['null_categories'][0]:,}")
    
    # Check which customers are missing categories
    logger.info("\nStep 2: Checking which customers are missing categories...")
    
    missing_categories_df = pl.from_pandas(pd.read_sql(
        """
        SELECT 
            IDCUST,
            TUTTLE_SALES_CATEGORY,
            COUNT(*) as record_count
        FROM TUTLIV.dbo.SAGE_MASTER_CATEGORIES
        WHERE TUTTLE_SALES_CATEGORY IS NULL OR TRIM(TUTTLE_SALES_CATEGORY) = ''
        GROUP BY IDCUST, TUTTLE_SALES_CATEGORY
        ORDER BY record_count DESC
        """,
        engine
    ))
    
    logger.info(f"Customers with missing categories: {len(missing_categories_df):,}")
    if len(missing_categories_df) > 0:
        logger.info("Top 10 customers with missing categories:")
        for i, row in enumerate(missing_categories_df.head(10).iter_rows()):
            logger.info(f"  {i+1}. Customer: {row[0]} | Records: {row[2]:,}")
    
    # Check the ALL_HSA_MKSEG table for customers without categories
    logger.info("\nStep 3: Checking ALL_HSA_MKSEG for customers without categories...")
    
    sage_sales_missing_df = pl.from_pandas(pd.read_sql(
        """
        DECLARE @curr_month_year VARCHAR(6);
        DECLARE @start_year VARCHAR(4);

        SET @curr_month_year = FORMAT(GETDATE(),'yyyyMM')
        SET @start_year = FORMAT(DATEADD(YEAR,-3,GETDATE()),'yyyy')

        SELECT 
            NEWBILLTO,
            COUNT(*) as sales_records,
            SUM(NETUNITS) as total_units,
            SUM(NETAMT) as total_amount
        FROM TUTLIV.dbo.ALL_HSA_MKSEG
        WHERE 
            (YEAR * 100 + MONTH) <> CAST(@curr_month_year AS INT) AND
            YEAR >= CAST(@start_year AS INT) AND
            TRIM(NAMECUST) NOT LIKE '%ingram book co.%' AND
            NEWBILLTO NOT IN (
                SELECT DISTINCT IDCUST 
                FROM TUTLIV.dbo.SAGE_MASTER_CATEGORIES 
                WHERE TUTTLE_SALES_CATEGORY IS NOT NULL AND TRIM(TUTTLE_SALES_CATEGORY) != ''
            )
        GROUP BY NEWBILLTO
        ORDER BY total_amount DESC
        """,
        engine
    ))
    
    logger.info(f"Customers in sales data without categories: {len(sage_sales_missing_df):,}")
    if len(sage_sales_missing_df) > 0:
        logger.info("Top 10 customers without categories (by sales amount):")
        for i, row in enumerate(sage_sales_missing_df.head(10).iter_rows()):
            logger.info(f"  {i+1}. Customer: {row[0]} | Records: {row[1]:,} | Units: {row[2]:,} | Amount: ${row[3]:,.2f}")
    
    # Check if there are any default categories we can use
    logger.info("\nStep 4: Checking available categories...")
    
    available_categories_df = pl.from_pandas(pd.read_sql(
        """
        SELECT 
            TUTTLE_SALES_CATEGORY,
            COUNT(*) as customer_count
        FROM TUTLIV.dbo.SAGE_MASTER_CATEGORIES
        WHERE TUTTLE_SALES_CATEGORY IS NOT NULL AND TRIM(TUTTLE_SALES_CATEGORY) != ''
        GROUP BY TUTTLE_SALES_CATEGORY
        ORDER BY customer_count DESC
        """,
        engine
    ))
    
    logger.info(f"Available categories: {len(available_categories_df):,}")
    logger.info("Top 10 categories by customer count:")
    for i, row in enumerate(available_categories_df.head(10).iter_rows()):
        logger.info(f"  {i+1}. {row[0]} | Customers: {row[1]:,}")
    
    # Provide recommendations
    logger.info("\n" + "=" * 80)
    logger.info("RECOMMENDATIONS TO FIX MISSING CATEGORIES")
    logger.info("=" * 80)
    
    if len(sage_sales_missing_df) > 0:
        logger.error(f"CRITICAL: {len(sage_sales_missing_df):,} customers in sales data are missing categories!")
        logger.error("This is causing the data loss in your pipeline.")
        
        logger.info("\nOPTIONS TO FIX:")
        logger.info("1. Add missing customers to SAGE_MASTER_CATEGORIES table with appropriate categories")
        logger.info("2. Use a default category (e.g., 'UNCATEGORIZED') for missing customers")
        logger.info("3. Filter out customers without categories (will lose data)")
        
        # Calculate impact of each option
        total_missing_records = sage_sales_missing_df['sales_records'].sum()
        total_missing_units = sage_sales_missing_df['total_units'].sum()
        total_missing_amount = sage_sales_missing_df['total_amount'].sum()
        
        logger.info(f"\nIMPACT ANALYSIS:")
        logger.info(f"  Missing records: {total_missing_records:,}")
        logger.info(f"  Missing units: {total_missing_units:,}")
        logger.info(f"  Missing amount: ${total_missing_amount:,.2f}")
        
        # Suggest default category approach
        logger.info(f"\nRECOMMENDED SOLUTION:")
        logger.info("Add a default category 'UNCATEGORIZED' to SAGE_MASTER_CATEGORIES for missing customers")
        logger.info("This will preserve all data while clearly marking uncategorized sales")
        
    else:
        logger.info("SUCCESS: All customers in sales data have categories!")
        logger.info("The issue might be in the join logic or data processing.")
    
    logger.info("=" * 80)

def create_default_categories():
    """
    Create default categories for customers missing from SAGE_MASTER_CATEGORIES.
    """
    logger.info("=" * 80)
    logger.info("CREATING DEFAULT CATEGORIES FOR MISSING CUSTOMERS")
    logger.info("=" * 80)
    
    # Find customers in sales data that are missing from categories table
    missing_customers_df = pl.from_pandas(pd.read_sql(
        """
        DECLARE @curr_month_year VARCHAR(6);
        DECLARE @start_year VARCHAR(4);

        SET @curr_month_year = FORMAT(GETDATE(),'yyyyMM')
        SET @start_year = FORMAT(DATEADD(YEAR,-3,GETDATE()),'yyyy')

        SELECT DISTINCT NEWBILLTO as IDCUST
        FROM TUTLIV.dbo.ALL_HSA_MKSEG
        WHERE 
            (YEAR * 100 + MONTH) <> CAST(@curr_month_year AS INT) AND
            YEAR >= CAST(@start_year AS INT) AND
            TRIM(NAMECUST) NOT LIKE '%ingram book co.%' AND
            NEWBILLTO NOT IN (
                SELECT DISTINCT IDCUST 
                FROM TUTLIV.dbo.SAGE_MASTER_CATEGORIES
            )
        """,
        engine
    ))
    
    logger.info(f"Found {len(missing_customers_df):,} customers missing from SAGE_MASTER_CATEGORIES")
    
    if len(missing_customers_df) > 0:
        # Create default category records
        default_categories = []
        for customer in missing_customers_df['IDCUST']:
            default_categories.append({
                'IDCUST': customer,
                'TUTTLE_SALES_CATEGORY': 'UNCATEGORIZED'
            })
        
        # Convert to DataFrame and insert
        default_df = pl.DataFrame(default_categories)
        
        logger.info(f"Creating {len(default_df):,} default category records...")
        
        try:
            # Insert into database
            pandas_df = default_df.to_pandas()
            pandas_df.to_sql(
                name='SAGE_MASTER_CATEGORIES',
                con=engine,
                schema='dbo',
                if_exists='append',
                index=False,
                chunksize=1000
            )
            
            logger.info(f"SUCCESS: Added {len(default_df):,} default category records")
            logger.info("All customers now have categories assigned")
            
        except Exception as e:
            logger.error(f"Error inserting default categories: {e}")
            logger.error("You may need to manually add these customers to the SAGE_MASTER_CATEGORIES table")
    
    else:
        logger.info("No missing customers found - all customers already have categories")

if __name__ == "__main__":
    investigate_sage_categories()
    
    # Ask user if they want to create default categories
    response = input("\nDo you want to create default categories for missing customers? (y/n): ")
    if response.lower() == 'y':
        create_default_categories()
    else:
        logger.info("Skipping default category creation. You can run this manually later.") 