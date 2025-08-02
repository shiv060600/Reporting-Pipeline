#type: ignore
import pandas as pd
import numpy as np
from dbfread import DBF
import sqlalchemy
import xlwings as xw
import urllib
from helpers.paths import PATHS
import datetime
import json
from rapidfuzz import process, fuzz
import sys
import time
import sqlite3
import polars as pl
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("reporting_pipeline.log",mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

SSMS_CONN_STRING = PATHS["SSMS_CONN_STRING"]
INGRAM_SALES_DBF = PATHS["INGRAM_SALES_DBF"]
INGRAM_CUSTCODES = PATHS["ACCT_CODES_DESCR_INGRAM"]
ISBN_WEBCAT = PATHS["ISBN_WEBCAT"]
SAGE_CUST_CODES = PATHS["SAGE_CUST_CODES"]
JSON_CUST_CODES = PATHS["JSON_CUST_CODES"]
EXPORT_XL = PATHS["ALL_SALES_INCL_ING"] 
DB_PATH = PATHS["DB_PATH"]
ING_QUERY = PATHS["ING_QUERY"]
SAGE_QUERY = PATHS["SAGE_QUERY"]

params = urllib.parse.quote_plus(SSMS_CONN_STRING)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

"""
This python file will be run every month after INGRAM sales data is updated, and the INGRAM Sales Table TUTLIV.dbo.ING_SALES.

This Program will Join INGRAM sales with SAGE Sales for detailed reporting.

TUTLIV.dbo.ING_SALES provides the last 5 years of Sales data grouped by each combination of ISBN,CUSTNAME,TITLE,YEAR,MONTH.
In other words, each row of data is all the sales (NETAMT,NETQY) for a book sold by a customer in that year month combination.


"""

def main():
    """
    Begin by getting all INGRAM Sales data for the last 3 years not including the current month

    COLUMNS of TUTLIV.dbo.ING_SALES:
    ISBN    YEAR    MONTH   TITLE   NAMECUST    NETUNITS    NETAMT
    """

    ingram_sales_df = pl.from_pandas(pd.read_sql(ING_QUERY, engine))

    """
    Next, grab all SAGE Sales data for the last 3 years not including current month, also include no sales where namecust LIKE 'INGRAM BOOK CO.'

    COLUMNS of TUTLIV.dbo.ALL_HSA_MKSEG:
    NETAMT    NETUNITS     NEWBILLTO    ISBN    YEAR    MONTH   TITLE   NAMECUST    IDACCTSET 
    """

    sage_sales_df = pl.from_pandas(pd.read_sql(SAGE_QUERY, engine))
    
    """
    WE DONT NEED THIS ANYMORE AFTER MAPPING TABLES
    # Get unique customer names for mapping
    unique_sage_names = sage_sales_df.select('NAMECUST').unique().get_column('NAMECUST')
    unique_ingram_names = ingram_sales_df.select('NAMECUST').unique().get_column('NAMECUST')
    
    # Apply name mapping
    logger.info("Generating name mappings between Ingram and Sage customers")
    name_mapping = rapid_fuzz_name_matching(unique_sage_names, unique_ingram_names)
    
    # Apply the mapping to create a new column
    logger.info("Applying name mappings to Ingram sales data")
    ingram_sales_df = ingram_sales_df.with_columns(
        pl.lit(None).alias('SAGE_COMPANY_NAME')
    )
    
    # Apply mapping (needs to convert to pandas temporarily as Polars doesn't have direct map functionality)
    temp_df = ingram_sales_df.to_pandas()
    temp_df['SAGE_COMPANY_NAME'] = temp_df['NAMECUST'].map(name_mapping)
    ingram_sales_df = pl.from_pandas(temp_df)
    
    logger.info(f"Name mapping applied to {len(ingram_sales_df)} Ingram sales records")
    
    # Replace NAMECUST with SAGE_COMPANY_NAME
    logger.info("Replacing NAMECUST with mapped SAGE_COMPANY_NAME in Ingram data")
    ingram_sales_df = ingram_sales_df.with_columns(
        pl.col("SAGE_COMPANY_NAME").alias("NAMECUST_MAPPED")
    )
    
    # Drop the original columns we don't need anymore
    ingram_sales_df = ingram_sales_df.drop(["NAMECUST", "SAGE_COMPANY_NAME"])
    ingram_sales_df = ingram_sales_df.rename({"NAMECUST_MAPPED": "NAMECUST"})
    """
    
    logger.info(f"Ingram columns: {ingram_sales_df.columns}")
    logger.info(f"Sage columns: {sage_sales_df.columns}")
    
    column_order = ['ISBN', 'YEAR', 'MONTH', 'TITLE', 'NAMECUST', 'NETUNITS', 'NETAMT', 'TUTTLE_SALES_CATEGORY']
    ingram_sales_df = ingram_sales_df.select(column_order)
    sage_sales_df = sage_sales_df.select(column_order)
    
    logger.info("Standardizing data types between dataframes")
    ingram_sales_df = ingram_sales_df.with_columns([
        pl.col('ISBN').cast(pl.Utf8).str.strip_chars(),
        pl.col('YEAR').cast(pl.Int64),
        pl.col('MONTH').cast(pl.Int64),
        pl.col('TITLE').cast(pl.Utf8),
        pl.col('NAMECUST').cast(pl.Utf8),
        pl.col('NETUNITS').cast(pl.Int64),
        pl.col('NETAMT').cast(pl.Float64),
        pl.col('TUTTLE_SALES_CATEGORY').cast(pl.Utf8)
    ])
    
    sage_sales_df = sage_sales_df.with_columns([
        pl.col('ISBN').cast(pl.Utf8).str.strip_chars(),
        pl.col('YEAR').cast(pl.Int64),
        pl.col('MONTH').cast(pl.Int64),
        pl.col('TITLE').cast(pl.Utf8),
        pl.col('NAMECUST').cast(pl.Utf8),
        pl.col('NETUNITS').cast(pl.Int64),
        pl.col('NETAMT').cast(pl.Float64),
        pl.col('TUTTLE_SALES_CATEGORY').cast(pl.Utf8)
    ])
    
    logger.info(f"Ingram schema: {ingram_sales_df.schema}")
    logger.info(f"Sage schema: {sage_sales_df.schema}")
    
    logger.info(f"Ingram dataframe shape: {ingram_sales_df.shape}")
    logger.info(f"Ingram total NETUNITS: {ingram_sales_df['NETUNITS'].sum()}")
    logger.info(f"Ingram total NETAMT: {ingram_sales_df['NETAMT'].sum()}")
    
    logger.info(f"Sage dataframe shape: {sage_sales_df.shape}")
    logger.info(f"Sage total NETUNITS: {sage_sales_df['NETUNITS'].sum()}")
    logger.info(f"Sage total NETAMT: {sage_sales_df['NETAMT'].sum()}")

    sage_and_ingram_sales = pl.concat([ingram_sales_df, sage_sales_df])

    logger.info(f"Combined dataframe shape: {sage_and_ingram_sales.shape}")
    logger.info(f"Combined total NETUNITS: {sage_and_ingram_sales['NETUNITS'].sum()}")
    logger.info(f"Combined total NETAMT: {sage_and_ingram_sales['NETAMT'].sum()}")
    
    logger.info("Grouping data by ISBN, YEAR, MONTH, TITLE, NAMECUST, and TUTTLE_SALES_CATEGORY")
    sage_and_ingram_sales = sage_and_ingram_sales.group_by(['ISBN','YEAR','MONTH','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY']).agg([
        pl.col('NETUNITS').sum().alias('NETUNITS'),
        pl.col('NETAMT').sum().alias('NETAMT')
        ])
    
    logger.info(f"Grouped dataframe shape: {sage_and_ingram_sales.shape}")
    logger.info(f"Grouped total NETUNITS: {sage_and_ingram_sales['NETUNITS'].sum()}")
    logger.info(f"Grouped total NETAMT: {sage_and_ingram_sales['NETAMT'].sum()}")
    
    sage_and_ingram_sales = sage_and_ingram_sales.with_columns(
        (pl.col('YEAR') * 100 + pl.col('MONTH')).alias('YEARMONTH')
    )

    # Fix the base_df creation to include TUTTLE_SALES_CATEGORY
    base_df = sage_and_ingram_sales.select(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY']).unique()

    # Add validation logging
    logger.info(f"Base dataframe shape: {base_df.shape}")
    logger.info(f"Base dataframe unique combinations: {len(base_df)}")

    curr_date = datetime.datetime.now()
    curr_month = curr_date.month
    curr_year = curr_date.year

    # Store original totals for validation
    original_combined_units = sage_and_ingram_sales['NETUNITS'].sum()
    original_combined_dollars = sage_and_ingram_sales['NETAMT'].sum()
    logger.info(f"Original combined totals - Units: {original_combined_units:,} | Dollars: ${original_combined_dollars:,.2f}")

    ytd_values = (sage_and_ingram_sales.filter(pl.col('YEAR') == curr_year)
            .group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'])
            .agg([
                pl.col('NETUNITS').sum().alias('YTD_UNITS'),
                pl.col('NETAMT').sum().alias('YTD_DOLLARS')
            ]))

    # Add validation for YTD values
    logger.info(f"YTD values shape: {ytd_values.shape}")
    logger.info(f"YTD total units: {ytd_values['YTD_UNITS'].sum():,}")
    logger.info(f"YTD total dollars: ${ytd_values['YTD_DOLLARS'].sum():,.2f}")

    report_df = base_df.join(ytd_values, on=['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'], how='left')
    report_df = report_df.with_columns([
        pl.col('YTD_DOLLARS').fill_null(0),
        pl.col('YTD_UNITS').fill_null(0)
    ])

    # Validate the join
    logger.info(f"Report dataframe after YTD join: {report_df.shape}")
    logger.info(f"Report YTD units: {report_df['YTD_UNITS'].sum():,}")
    logger.info(f"Report YTD dollars: ${report_df['YTD_DOLLARS'].sum():,.2f}")

    twelve_months_prior = []
    monthly_column_names = []
    
    for i in range(1,13):
        month = curr_month - i
        year = curr_year
        if month <= 0:
            month += 12
            year -= 1
        year_month = (year * 100) + month
        twelve_months_prior.append(year_month)
        
        # Create monthly column names in format: NET_UNITS_MMM_YYYY
        month_date = datetime.datetime(year, month, 1)
        column_name = f"NET_UNITS_{month_date.strftime('%b')}_{year}"
        monthly_column_names.append(column_name)
        
        logger.info(f"Creating monthly column: {column_name} for {month_date.strftime('%B %Y')}")
        
        month_values = (sage_and_ingram_sales
                       .filter(pl.col('YEARMONTH') == year_month)
                       .group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'])
                       .agg([pl.col('NETUNITS').sum().alias(column_name)])
                      )
        
        # Validate monthly values
        logger.info(f"{column_name} shape: {month_values.shape}")
        logger.info(f"{column_name} total: {month_values[column_name].sum():,}")
        
        report_df = report_df.join(month_values, on=['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'], how='left')
        report_df = report_df.with_columns(
            pl.col(column_name).fill_null(0)
        )
        
        if column_name in report_df.columns:
            logger.info(f"Successfully added column {column_name}")
            logger.info(f"Report {column_name} total: {report_df[column_name].sum():,}")
        else:
            logger.error(f"Failed to add column {column_name}")
    
    logger.info(f"Created {len(monthly_column_names)} monthly columns: {monthly_column_names}")
    
    # Verify the columns exist and have data
    for col in monthly_column_names:
        if col in report_df.columns:
            non_zero = report_df.filter(pl.col(col) > 0).height
            logger.info(f"Column {col} exists with {non_zero} non-zero values")
        else:
            logger.error(f"Column {col} is missing from the dataframe")
    
    #add 12M totals (units,sales)
    twelve_month_values = (sage_and_ingram_sales.filter(pl.col('YEARMONTH').is_in(twelve_months_prior))
                          .group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'])
                          .agg([
                              pl.col('NETAMT').sum().alias('12M_DOLLARS'),
                              pl.col('NETUNITS').sum().alias('12M_UNITS')
                          ]))
    
    # Validate 12M values
    logger.info(f"12M values shape: {twelve_month_values.shape}")
    logger.info(f"12M total units: {twelve_month_values['12M_UNITS'].sum():,}")
    logger.info(f"12M total dollars: ${twelve_month_values['12M_DOLLARS'].sum():,.2f}")
    
    report_df = report_df.join(twelve_month_values, on=['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'], how='left')
    report_df = report_df.with_columns([
        pl.col('12M_DOLLARS').fill_null(0),
        pl.col('12M_UNITS').fill_null(0)
    ])
    
    # Validate after 12M join
    logger.info(f"Report dataframe after 12M join: {report_df.shape}")
    logger.info(f"Report 12M units: {report_df['12M_UNITS'].sum():,}")
    logger.info(f"Report 12M dollars: ${report_df['12M_DOLLARS'].sum():,.2f}")

    yearly_column_names = []
    
    for year in range(curr_year-3, curr_year):
        column_name = f"UNITS_{year}"
        yearly_column_names.append(column_name)
        
        logger.info(f"Creating yearly column: {column_name}")
        
        curr_year_units = (sage_and_ingram_sales
                          .filter(pl.col('YEAR') == year)
                          .group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'])
                          .agg(pl.col('NETUNITS').sum().alias(column_name)))
        
        # Validate yearly values
        logger.info(f"{column_name} shape: {curr_year_units.shape}")
        logger.info(f"{column_name} total: {curr_year_units[column_name].sum():,}")
        
        report_df = report_df.join(curr_year_units, on=['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'], how='left')
        report_df = report_df.with_columns(pl.col(column_name).fill_null(0))
        
        if column_name in report_df.columns:
            logger.info(f"Successfully added column {column_name}")
            logger.info(f"Report {column_name} total: {report_df[column_name].sum():,}")
        else:
            logger.error(f"Failed to add column {column_name}")
    
    logger.info(f"Created {len(yearly_column_names)} yearly columns: {yearly_column_names}")
    
    # Verify the columns exist and have data
    for col in yearly_column_names:
        if col in report_df.columns:
            non_zero = report_df.filter(pl.col(col) > 0).height
            logger.info(f"Column {col} exists with {non_zero} non-zero values")
        else:
            logger.error(f"Column {col} is missing from the dataframe")
    

    logger.info("Fetching additional data from SQL Server tables")
    
    try:
        logger.info("Fetching ALL_ACCOUNTS_12M_ROLL data")
        all_accounts_df = pl.from_pandas(pd.read_sql(
            """
            SELECT 
                TRIM(ITEMNO) as ISBN, 
                NETQTY as ALL_ACCTS_12M_UNITS, 
                NETSALES as ALL_ACCTS_12M_DOLLARS 
            FROM TUTLIV.dbo.ALL_ACCOUNTS_12M_ROLL
            """,
            engine
        ))
        
        all_accounts_df = all_accounts_df.with_columns([
            pl.col('ISBN').cast(pl.Utf8).str.strip_chars().str.replace(r'-', '')
        ])
        
        logger.info(f"Retrieved {len(all_accounts_df)} rows from ALL_ACCOUNTS_12M_ROLL")
    except Exception as e:
        logger.error(f"Error fetching ALL_ACCOUNTS_12M_ROLL data: {e}")
        all_accounts_df = pl.DataFrame({"ISBN": [], "ALL_ACCTS_12M_UNITS": [], "ALL_ACCTS_12M_DOLLARS": []})
    
    try:
        logger.info("Fetching BOOK_DETAILS data")
        book_details_df = pl.from_pandas(pd.read_sql(
            """
            SELECT 
                TRIM(ISBN) as ISBN, 
                PROD_TYPE as TYPE, 
                PROD_CLASS as PROD, 
                SEAS, 
                SUBPUB as SUB, 
                RETAIL_PRICE as RETAIL,
                TRIM(WEBCAT2) as WEBCAT2,
                TRIM(WEBCAT2_DESCR) as WEBCAT2_DESCR
            FROM TUTLIV.dbo.BOOK_DETAILS
            """,
            engine
        ))
        
        book_details_df = book_details_df.with_columns([
            pl.col('ISBN').cast(pl.Utf8).str.strip_chars().str.replace(r'-', '')
        ])
        
        logger.info(f"Retrieved {len(book_details_df)} rows from BOOK_DETAILS")
    except Exception as e:
        logger.error(f"Error fetching BOOK_DETAILS data: {e}")
        book_details_df = pl.DataFrame({"ISBN": [], "TYPE": [], "PROD": [], "SEAS": [], "SUB": [], "RETAIL": [],"WEBCAT2":[],"WEBCAT2_DESCR":[]})
    
    logger.info("Standardizing ISBNs in report data")
    report_df = report_df.with_columns([
        pl.col('ISBN').cast(pl.Utf8).str.strip_chars().str.replace(r'-', '')
    ])
    
    logger.info("Joining sales data with product details")
    
    report_df = report_df.join(all_accounts_df, on="ISBN", how="left")
    report_df = report_df.join(book_details_df, on="ISBN", how="left")
    
    report_df = report_df.with_columns([
        pl.col("ALL_ACCTS_12M_UNITS").fill_null(0),
        pl.col("ALL_ACCTS_12M_DOLLARS").fill_null(0),
        pl.col("TYPE").fill_null(""),
        pl.col("PROD").fill_null(""),
        pl.col("SEAS").fill_null(""),
        pl.col("SUB").fill_null(""),
        pl.col("RETAIL").fill_null(0),
        pl.col("WEBCAT2").fill_null(""),
        pl.col("WEBCAT2_DESCR").fill_null("")
    ])
    
    logger.info("Performing final groupby on ISBN, TITLE, NAMECUST, and TUTTLE_SALES_CATEGORY to eliminate duplicates")
    
    # Store pre-groupby totals for validation
    pre_groupby_ytd_units = report_df['YTD_UNITS'].sum() if 'YTD_UNITS' in report_df.columns else 0
    pre_groupby_ytd_dollars = report_df['YTD_DOLLARS'].sum() if 'YTD_DOLLARS' in report_df.columns else 0
    pre_groupby_12m_units = report_df['12M_UNITS'].sum() if '12M_UNITS' in report_df.columns else 0
    pre_groupby_12m_dollars = report_df['12M_DOLLARS'].sum() if '12M_DOLLARS' in report_df.columns else 0
    
    logger.info(f"All columns before final groupby: {report_df.columns}")
    logger.info(f"Pre-groupby shape: {report_df.shape}")
    logger.info(f"Pre-groupby YTD units: {pre_groupby_ytd_units:,}")
    logger.info(f"Pre-groupby YTD dollars: ${pre_groupby_ytd_dollars:,.2f}")
    
    # Build aggregation expressions for final groupby
    agg_expressions = []
    
    for col in ['TYPE', 'PROD', 'SUB', 'RETAIL', 'SEAS', 
               'ALL_ACCTS_12M_UNITS', 'ALL_ACCTS_12M_DOLLARS',
               'WEBCAT2', 'WEBCAT2_DESCR']:
        if col in report_df.columns:
            agg_expressions.append(pl.col(col).first().alias(col))
            logger.info(f"Adding attribute column to aggregation: {col}")
        else:
            logger.warning(f"Column {col} not found in dataframe")
    
    for col in ['12M_UNITS', '12M_DOLLARS', 'YTD_UNITS', 'YTD_DOLLARS']:
        if col in report_df.columns:
            agg_expressions.append(pl.col(col).sum().alias(col))
            logger.info(f"Adding metric column to aggregation: {col}")
        else:
            logger.warning(f"Column {col} not found in dataframe")
    
    actual_monthly_cols = [col for col in report_df.columns if col.startswith('NET_UNITS_')]
    for col in actual_monthly_cols:
        agg_expressions.append(pl.col(col).sum().alias(col))
        logger.info(f"Adding monthly column to aggregation: {col}")
    
    actual_yearly_cols = [col for col in report_df.columns if col.startswith('UNITS_') and col[-4:].isdigit()]
    for col in actual_yearly_cols:
        agg_expressions.append(pl.col(col).sum().alias(col))
        logger.info(f"Adding yearly column to aggregation: {col}")
    
    logger.info(f"Total aggregation expressions: {len(agg_expressions)}")
    logger.info(f"Monthly columns in aggregation: {len(actual_monthly_cols)}")
    logger.info(f"Yearly columns in aggregation: {len(actual_yearly_cols)}")
    
    # Use consistent grouping keys: ISBN, TITLE, NAMECUST, TUTTLE_SALES_CATEGORY
    report_df = report_df.group_by(['ISBN', 'TITLE', 'NAMECUST', 'TUTTLE_SALES_CATEGORY']).agg(agg_expressions)
    
    logger.info(f"Rows after final groupby: {len(report_df)}")
    
    # Store post-groupby totals for validation
    post_groupby_ytd_units = report_df['YTD_UNITS'].sum() if 'YTD_UNITS' in report_df.columns else 0
    post_groupby_ytd_dollars = report_df['YTD_DOLLARS'].sum() if 'YTD_DOLLARS' in report_df.columns else 0
    post_groupby_12m_units = report_df['12M_UNITS'].sum() if '12M_UNITS' in report_df.columns else 0
    post_groupby_12m_dollars = report_df['12M_DOLLARS'].sum() if '12M_DOLLARS' in report_df.columns else 0
    
    # Order columns for final output
    actual_monthly_cols_sorted = sorted([col for col in report_df.columns if col.startswith('NET_UNITS_')], 
                                       key=lambda x: datetime.datetime.strptime(f"{x.split('_')[2]} {x.split('_')[3]}", "%b %Y"), 
                                       reverse=True)
    actual_yearly_cols_sorted = sorted([col for col in report_df.columns if col.startswith('UNITS_') and col[-4:].isdigit()], 
                                      reverse=True)
    
    column_order = [
        "TITLE", "ISBN", "NAMECUST",
        "TUTTLE_SALES_CATEGORY",
        "TYPE",
        "PROD",
        "WEBCAT2",
        "WEBCAT2_DESCR",
        "SUB",
        "RETAIL",
        "SEAS",
        "ALL_ACCTS_12M_UNITS",
        "ALL_ACCTS_12M_DOLLARS",
        "12M_UNITS",
        "12M_DOLLARS",
        "YTD_UNITS",
        "YTD_DOLLARS"
    ] + actual_monthly_cols_sorted + actual_yearly_cols_sorted
    
    final_columns = [col for col in column_order if col in report_df.columns]
    
    remaining_columns = [col for col in report_df.columns if col not in final_columns]
    final_columns.extend(remaining_columns)
    
    logger.info(f"Final column order: {final_columns}")
    
    report_df = report_df.select(final_columns)
    
    logger.info("Verifying final column order:")
    for i, col in enumerate(report_df.columns):
        logger.info(f"{i+1}. {col}")
    
    # Verify WEBCAT2 columns are present and have data
    if 'WEBCAT2' in report_df.columns:
        webcat2_non_empty = report_df.filter(pl.col('WEBCAT2') != "").height
        logger.info(f"WEBCAT2 column present with {webcat2_non_empty} non-empty values")
    else:
        logger.error("WEBCAT2 column is missing from final output!")
    
    if 'WEBCAT2_DESCR' in report_df.columns:
        webcat2_descr_non_empty = report_df.filter(pl.col('WEBCAT2_DESCR') != "").height
        logger.info(f"WEBCAT2_DESCR column present with {webcat2_descr_non_empty} non-empty values")
    else:
        logger.error("WEBCAT2_DESCR column is missing from final output!")
    
    # COMPREHENSIVE DATA VALIDATION - COMPARE ORIGINAL VS FINAL TOTALS
    logger.info("=" * 80)
    logger.info("COMPREHENSIVE DATA VALIDATION SUMMARY")
    logger.info("=" * 80)
    
    # Original data totals (from individual DataFrames)
    original_ingram_units = ingram_sales_df['NETUNITS'].sum()
    original_ingram_dollars = ingram_sales_df['NETAMT'].sum()
    original_sage_units = sage_sales_df['NETUNITS'].sum()
    original_sage_dollars = sage_sales_df['NETAMT'].sum()
    original_combined_units = original_ingram_units + original_sage_units
    original_combined_dollars = original_ingram_dollars + original_sage_dollars
    
    # Final output totals
    final_ytd_units = report_df['YTD_UNITS'].sum() if 'YTD_UNITS' in report_df.columns else 0
    final_ytd_dollars = report_df['YTD_DOLLARS'].sum() if 'YTD_DOLLARS' in report_df.columns else 0
    final_12m_units = report_df['12M_UNITS'].sum() if '12M_UNITS' in report_df.columns else 0
    final_12m_dollars = report_df['12M_DOLLARS'].sum() if '12M_DOLLARS' in report_df.columns else 0
    
    logger.info("ORIGINAL DATA TOTALS:")
    logger.info(f"  Ingram Sales - Units: {original_ingram_units:,} | Dollars: ${original_ingram_dollars:,.2f}")
    logger.info(f"  Sage Sales   - Units: {original_sage_units:,} | Dollars: ${original_sage_dollars:,.2f}")
    logger.info(f"  Combined     - Units: {original_combined_units:,} | Dollars: ${original_combined_dollars:,.2f}")
    
    logger.info("\nFINAL REPORT TOTALS:")
    logger.info(f"  YTD Totals   - Units: {final_ytd_units:,} | Dollars: ${final_ytd_dollars:,.2f}")
    logger.info(f"  12M Totals   - Units: {final_12m_units:,} | Dollars: ${final_12m_dollars:,.2f}")
    
    logger.info("\nGROUPBY VALIDATION:")
    logger.info(f"  Pre-groupby  - YTD Units: {pre_groupby_ytd_units:,} | YTD Dollars: ${pre_groupby_ytd_dollars:,.2f}")
    logger.info(f"  Post-groupby - YTD Units: {post_groupby_ytd_units:,} | YTD Dollars: ${post_groupby_ytd_dollars:,.2f}")
    logger.info(f"  Pre-groupby  - 12M Units: {pre_groupby_12m_units:,} | 12M Dollars: ${pre_groupby_12m_dollars:,.2f}")
    logger.info(f"  Post-groupby - 12M Units: {post_groupby_12m_units:,} | 12M Dollars: ${post_groupby_12m_dollars:,.2f}")
    
    # Calculate variance percentages
    ytd_units_diff = abs(pre_groupby_ytd_units - post_groupby_ytd_units)
    ytd_dollars_diff = abs(pre_groupby_ytd_dollars - post_groupby_ytd_dollars)
    ytd_units_variance = (ytd_units_diff / pre_groupby_ytd_units * 100) if pre_groupby_ytd_units > 0 else 0
    ytd_dollars_variance = (ytd_dollars_diff / pre_groupby_ytd_dollars * 100) if pre_groupby_ytd_dollars > 0 else 0
    
    logger.info(f"\nVARIANCE ANALYSIS:")
    logger.info(f"  YTD Units variance: {ytd_units_variance:.2f}%")
    logger.info(f"  YTD Dollars variance: {ytd_dollars_variance:.2f}%")
    
    if ytd_units_variance > 2.0 or ytd_dollars_variance > 2.0:
        logger.warning("WARNING: Variance exceeds 2% - investigate data integrity!")
    else:
        logger.info("Data integrity validated - variance within acceptable range")
    
    logger.info(f"\nFINAL DATA SUMMARY:")
    logger.info(f"  Total rows in final report: {len(report_df):,}")
    logger.info(f"  Original combined records: {len(sage_and_ingram_sales):,}")
    logger.info(f"  Data retention rate: {(len(report_df) / len(sage_and_ingram_sales) * 100):.2f}%")
    
    # Calculate data loss
    expected_rows = len(sage_and_ingram_sales.select(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY']).unique())
    actual_rows = len(report_df)
    data_loss = expected_rows - actual_rows
    
    logger.info(f"  Expected unique combinations: {expected_rows:,}")
    logger.info(f"  Actual rows in report: {actual_rows:,}")
    logger.info(f"  Data loss: {data_loss:,} rows ({data_loss/expected_rows*100:.2f}%)")
    
    if data_loss > 0:
        logger.warning(f"WARNING: {data_loss:,} rows were lost during processing!")
        logger.warning("This indicates potential issues with joins or grouping operations.")
    else:
        logger.info("SUCCESS: No data loss detected!")
    
    logger.info("=" * 80)
    
    logger.info("Uploading to SQL Server test table for validation")

    try:
        pandas_df = report_df.to_pandas()
        # Production table upload
        logger.info("Exporting results to SQL Server (TUTLIV database)")
        production_table_name = "COMBINED_SALES_REPORT"
        schema = "dbo"
        logger.info(f"Writing to SQL Server table: {schema}.{production_table_name}")
        pandas_df.to_sql(
            name=production_table_name,
            con=engine,
            schema=schema,
            if_exists='replace',
            index=False
        )
        logger.info(f"Successfully exported {len(pandas_df)} rows to {schema}.{production_table_name}")
        
    except Exception as e:
        logger.error(f"Error uploading to SQL Server test table: {e}")
    finally:
        engine.dispose()
        
    logger.info("Processing completed successfully")


if __name__ == "__main__":
    logger.info("Starting the process of combining Sage and Ingram sales")
    main()
    logger.info("Finished combining data and reporting logic")
    time.sleep(3)
    sys.exit(0)
