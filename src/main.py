#type: ignore
import pandas as pd
import numpy as np
from dbfread import DBF
import sqlalchemy
import xlwings as xw
import urllib
from helpers.paths import PATHS
from helpers.paths import ING_QUERY, SAGE_QUERY
import datetime
import json
from rapidfuzz import process, fuzz
import sys
import time
import sqlite3
import polars as pl
import logging
import os

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
TARGET_CALCULATIONS_FILE = PATHS["TARGET_CALCULATION_FILE"]



"""
This python file will be run every month after INGRAM sales data is updated, and the INGRAM Sales Table TUTLIV.dbo.ING_SALES.

This Program will Join INGRAM sales with SAGE Sales for detailed reporting.

TUTLIV.dbo.ING_SALES provides the last 5 years of Sales data grouped by each combination of ISBN,CUSTNAME,TITLE,YEAR,MONTH.
In other words, each row of data is all the sales (NETAMT,NETQY) for a book sold by a customer in that year month combination.
"""  
def combined_sales_report(ingram_sales_df:pl.DataFrame,sage_sales_df:pl.DataFrame):

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
        pl.col('TITLE').cast(pl.Utf8).str.strip_chars(),
        pl.col('NAMECUST').cast(pl.Utf8).str.strip_chars(),
        pl.col('NETUNITS').cast(pl.Int64),
        pl.col('NETAMT').cast(pl.Float64),
        pl.col('TUTTLE_SALES_CATEGORY').cast(pl.Utf8).str.strip_chars()
    ])
    
    sage_sales_df = sage_sales_df.with_columns([
        pl.col('ISBN').cast(pl.Utf8).str.strip_chars(),
        pl.col('YEAR').cast(pl.Int64),
        pl.col('MONTH').cast(pl.Int64),
        pl.col('TITLE').cast(pl.Utf8).str.strip_chars(),
        pl.col('NAMECUST').cast(pl.Utf8).str.strip_chars(),
        pl.col('NETUNITS').cast(pl.Int64),
        pl.col('NETAMT').cast(pl.Float64),
        pl.col('TUTTLE_SALES_CATEGORY').cast(pl.Utf8).str.strip_chars()
    ])
    
    ingram_net_sales_before_concat = ingram_sales_df["NETAMT"].sum()
    ingram_net_units_before_concat = ingram_sales_df["NETUNITS"].sum()
    sage_net_sales_before_concat = sage_sales_df["NETAMT"].sum()
    sage_net_units_before_concat = sage_sales_df["NETUNITS"].sum()

    logger.info('contating sage and ingram sales (Vstack,concat)')
    sage_and_ingram_sales = pl.concat([ingram_sales_df, sage_sales_df])
    
    logger.info("Grouping data by ISBN, YEAR, MONTH, TITLE, NAMECUST, and TUTTLE_SALES_CATEGORY")
    sage_and_ingram_sales = sage_and_ingram_sales.group_by(['ISBN','YEAR','MONTH','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY']).agg([
        pl.col('NETUNITS').sum().alias('NETUNITS'),
        pl.col('NETAMT').sum().alias('NETAMT')
        ])
    
    sage_and_ingram_sales = sage_and_ingram_sales.with_columns(
        (pl.col('YEAR') * 100 + pl.col('MONTH')).alias('YEARMONTH')
    )

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


    logger.info(f"Totals before concat - Units:{ingram_net_units_before_concat+sage_net_units_before_concat} | Dollars: {ingram_net_sales_before_concat+sage_net_units_before_concat}")
    logger.info(f"Totals After Contat - Units: {original_combined_units:,} | Dollars: ${original_combined_dollars:,.2f}")

    logger.info(f"Diff- Units : {original_combined_units - (ingram_net_units_before_concat + sage_net_units_before_concat)} | Dollars : {original_combined_dollars - (ingram_net_sales_before_concat + sage_net_sales_before_concat)}")


    # Get ytd sales and units
    ytd_values = (sage_and_ingram_sales.filter(pl.col('YEAR') == curr_year)
            .group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'])
            .agg([
                pl.col('NETUNITS').sum().alias('YTD_UNITS'),
                pl.col('NETAMT').sum().alias('YTD_DOLLARS')
            ]))

    report_df = base_df.join(ytd_values, on=['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'], how='left')
    report_df = report_df.with_columns([
        pl.col('YTD_DOLLARS').fill_null(0),
        pl.col('YTD_UNITS').fill_null(0)
    ])

    twelve_months_prior = []
    monthly_column_names = []
    
    #dynamically including last twelve months of sold units 
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
    
    #add rolling 12M totals (units,sales)
    twelve_month_values = (sage_and_ingram_sales.filter(pl.col('YEARMONTH').is_in(twelve_months_prior))
                          .group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'])
                          .agg([
                              pl.col('NETAMT').sum().alias('12M_DOLLARS'),
                              pl.col('NETUNITS').sum().alias('12M_UNITS')
                          ]))
    report_df = report_df.join(twelve_month_values, on=['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'], how='left')
    report_df = report_df.with_columns([
        pl.col('12M_DOLLARS').fill_null(0),
        pl.col('12M_UNITS').fill_null(0)
    ])
    
    # Get last 4 months units dollars rolling
    four_months_prior = twelve_months_prior[:4]
    four_month_values = (sage_and_ingram_sales.filter(pl.col('YEARMONTH').is_in(four_months_prior))
                         .group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'])
                         .agg([
                             pl.col('NETAMT').sum().alias('4M_DOLLARS'),
                             pl.col('NETUNITS').sum().alias('4M_UNITS')
                         ]))
    report_df = report_df.join(four_month_values, on=['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'], how='left')
    report_df = report_df.with_columns([
        pl.col('4M_DOLLARS').fill_null(0),
        pl.col('4M_UNITS').fill_null(0)
    ])    

    yearly_column_names = []
    for year in range(curr_year-3, curr_year):
        column_name = f"UNITS_{year}"
        yearly_column_names.append(column_name)
        
        curr_year_units = (sage_and_ingram_sales
                          .filter(pl.col('YEAR') == year)
                          .group_by(['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'])
                          .agg(pl.col('NETUNITS').sum().alias(column_name)))
        
        report_df = report_df.join(curr_year_units, on=['ISBN','TITLE','NAMECUST','TUTTLE_SALES_CATEGORY'], how='left')
        report_df = report_df.with_columns(pl.col(column_name).fill_null(0))
        
        if column_name in report_df.columns:
            logger.info(f"Successfully added column {column_name}")
            logger.info(f"Report {column_name} total: {report_df[column_name].sum():,}")
        else:
            logger.error(f"Failed to add column {column_name}")
    
    
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
                PUB_STATUS as PUB_STATUS,
                SEAS, 
                SUB_PUB as SUB, 
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
    logger.info(f"Pre-groupby_12M_units: {pre_groupby_12m_units:,.2f}")
    logger.info(f"Pre-groupby_12M_dollars: {pre_groupby_12m_dollars:,.2f}")
    
    # Build aggregation expressions for final groupby
    agg_expressions = []
    
    for col in ['TYPE', 'PROD', 'SUB', 'RETAIL', 'SEAS', 
               'ALL_ACCTS_12M_UNITS', 'ALL_ACCTS_12M_DOLLARS',
               'WEBCAT2', 'WEBCAT2_DESCR','PUB_STATUS']:
        if col in report_df.columns:
            agg_expressions.append(pl.col(col).first().alias(col))
            logger.info(f"Adding attribute column to aggregation: {col}")
        else:
            logger.warning(f"Column {col} not found in dataframe")
    
    for col in ['12M_UNITS', '12M_DOLLARS', 'YTD_UNITS', 'YTD_DOLLARS','4M_UNITS', '4M_DOLLARS']:
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
    
    
    report_df = report_df.group_by(['ISBN', 'TITLE', 'NAMECUST', 'TUTTLE_SALES_CATEGORY']).agg(agg_expressions)
    
    logger.info(f"Rows after final groupby: {len(report_df)}")
    
    # Store post-groupby totals for validation
    post_groupby_ytd_units = report_df['YTD_UNITS'].sum() if 'YTD_UNITS' in report_df.columns else 0
    post_groupby_ytd_dollars = report_df['YTD_DOLLARS'].sum() if 'YTD_DOLLARS' in report_df.columns else 0
    post_groupby_12m_units = report_df['12M_UNITS'].sum() if '12M_UNITS' in report_df.columns else 0 
    post_groupby_12m_dollars = report_df['12M_DOLLARS'].sum() if '12M_DOLLARS' in report_df.columns else 0

    #Log differences
    logger.info(f"YTD Diff - Units: {(pre_groupby_ytd_units - post_groupby_ytd_units)} | Dollars {(pre_groupby_ytd_dollars - post_groupby_ytd_dollars):,.2f}")
    logger.info(f"12MRoll Diff - Units: {(pre_groupby_12m_units - post_groupby_12m_units)} | Dollars {(pre_groupby_12m_dollars - post_groupby_12m_dollars):,.2f}")

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
        "PUB_STATUS",
        "RETAIL",
        "SEAS",
        "ALL_ACCTS_12M_UNITS",
        "ALL_ACCTS_12M_DOLLARS",
        "12M_UNITS",
        "12M_DOLLARS",
        "YTD_UNITS",
        "YTD_DOLLARS"
    ] + actual_monthly_cols_sorted + actual_yearly_cols_sorted + ["4M_UNITS", "4M_DOLLARS"]
    
    final_columns = [col for col in column_order if col in report_df.columns]
    
    remaining_columns = [col for col in report_df.columns if col not in final_columns]
    final_columns.extend(remaining_columns)
    
    logger.info(f"Final column order: {final_columns}")
    
    report_df = report_df.select(final_columns)

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

def report_three_combined(ingram_sales_df: pl.DataFrame,sage_sales_df: pl.DataFrame,target_calculations_df: pl.DataFrame):
    #Order and standardize data (need IDs for mapping multiplication)
    column_order_ing = ['HQ_NUMBER','SL_NUMBER', 'YEAR', 'MONTH',  'NAMECUST', 'NETUNITS', 'NETAMT', 'TUTTLE_SALES_CATEGORY']
    column_order_sage = ['SAGE_ID', 'YEAR', 'MONTH', 'NAMECUST', 'NETUNITS', 'NETAMT', 'TUTTLE_SALES_CATEGORY']
    column_order_target_calculations  = ['BILLTO','MUL_RATIO']
    ingram_sales_df = ingram_sales_df.select(column_order_ing)
    sage_sales_df = sage_sales_df.select(column_order_sage)
    target_calculations_df = target_calculations_df.select(column_order_target_calculations)

    target_calculations_df =  target_calculations_df.drop_nulls(subset=['BILLTO','MUL_RATIO'])
    target_calculations_df = target_calculations_df.filter(
        (pl.col("BILLTO") != '') & (pl.col("MUL_RATIO") != 0.0)
    )

    target_calculations_df = target_calculations_df.with_columns(
        pl.col("BILLTO").cast(pl.Utf8).str.strip_chars().replace(r"[A-Za-z]+$",''), #remove the category characters from the end
        pl.col("MUL_RATIO").cast(pl.Float64)
    )
    ingram_sales_df = ingram_sales_df.with_columns(
        pl.col("YEAR").cast(pl.Int64),
        pl.col("MONTH").cast(pl.Int64),
        pl.col("NETUNITS").cast(pl.Int64),
        pl.col("NETAMT").cast(pl.Int64)
    ).with_columns(
        (pl.col("YEAR") * 100 + pl.col("MONTH")).alias("YEARMONTH")
    ).with_columns(
        pl.col("HQ_NUMBER").cast(pl.Utf8).str.strip_chars(),
        pl.col("SL_NUMBER").cast(pl.Utf8).str.strip_chars(),
        pl.col("NAMECUST").cast(pl.Utf8).str.strip_chars(),
        pl.col("TUTTLE_SALES_CATEGORY").cast(pl.Utf8).str.strip_chars()
    )
    sage_sales_df = sage_sales_df.with_columns(
        pl.col("YEAR").cast(pl.Int64),
        pl.col("MONTH").cast(pl.Int64),
        pl.col("NETUNITS").cast(pl.Int64),
        pl.col("NETAMT").cast(pl.Int64)
    ).with_columns(
        (pl.col("YEAR") * 100 + pl.col("MONTH")).alias("YEARMONTH")
    ).with_columns(
        pl.col("SAGE_ID").cast(pl.Utf8).str.strip_chars(),
        pl.col("NAMECUST").cast(pl.Utf8).str.strip_chars(),
        pl.col("TUTTLE_SALES_CATEGORY").cast(pl.Utf8).str.strip_chars()
    )

    ingram_sales_df = ingram_sales_df.join(
        target_calculations_df,
        left_on = 'HQ_NUMBER',
        right_on = 'BILLTO',
        how = 'left'
    )
    
    ingram_sales_df = ingram_sales_df.with_columns(
        (pl.col("NETAMT") * pl.col("MUL_RATIO").fill_null(1.0)).cast(pl.Float64).alias("TARGET_NETAMT")
    )
    
    # drop columns that exist
    cols_to_drop = [col for col in ["MUL_RATIO", "BILLTO"] if col in ingram_sales_df.columns]
    if cols_to_drop:
        ingram_sales_df = ingram_sales_df.drop(cols_to_drop)

    sage_sales_df = sage_sales_df.join(
        target_calculations_df,
        left_on = 'SAGE_ID',
        right_on = 'BILLTO',
        how = 'left'
    ).with_columns(
        (pl.col('NETAMT') * pl.col('MUL_RATIO').fill_null(1.0)).cast(pl.Float64).alias("TARGET_NETAMT")
    )
    
    # drop columns that exist
    cols_to_drop = [col for col in ["MUL_RATIO", "BILLTO"] if col in sage_sales_df.columns]
    if cols_to_drop:
        sage_sales_df = sage_sales_df.drop(cols_to_drop)

    #drop ID columns for concant
    ingram_sales_df = ingram_sales_df.drop(["HQ_NUMBER","SL_NUMBER"])
    sage_sales_df = sage_sales_df.drop(["SAGE_ID"])


    #logic for Erics request of adding a '*' next to cusomter who are both from IPS (SAGE) and INGWS (ING)
    sage_customers = set(sage_sales_df["NAMECUST"].unique().to_list())
    ingram_customers = set((ingram_sales_df["NAMECUST"].unique().to_list()))
    customers_in_both = sage_customers.intersection(ingram_customers) #set addition
    #create function to add '*'
    def add_star(value):
        if value in customers_in_both:
            return value + '*'
        else:
            return value

    sage_sales_df = sage_sales_df.with_columns(
        pl.col("NAMECUST").map_elements(add_star,return_dtype=pl.Utf8)
    )
    ingram_sales_df = ingram_sales_df.with_columns(
        pl.col("NAMECUST").map_elements(add_star,return_dtype=pl.Utf8)
    )

    combined_df = pl.concat([sage_sales_df,ingram_sales_df])
    
    # Two distinct grouping key lists for clarity
    full_grouping_keys = ["NAMECUST","YEAR","MONTH","YEARMONTH","TUTTLE_SALES_CATEGORY"]  # For aggregating with time dimensions
    base_grouping_keys = ["NAMECUST","TUTTLE_SALES_CATEGORY"]  # For report structure (no time dimensions)
    
    aggregate_expressions = [
        pl.col("NETAMT").sum().alias("NETAMT"),
        pl.col("NETUNITS").sum().alias("NETUNITS"),
        pl.col("TARGET_NETAMT").sum().alias("TARGET_NETAMT")
    ]

    combined_df = combined_df.group_by(full_grouping_keys).agg(aggregate_expressions)
    base_df = combined_df.select(base_grouping_keys).unique()
    curr_date = datetime.datetime.now()

    ytd_values = combined_df.filter(
        pl.col("YEAR") == curr_date.year
    ).group_by(base_grouping_keys).agg([
        pl.col("NETAMT").sum().alias("YTD_ACTUAL"),
        pl.col("TARGET_NETAMT").sum().alias("YTD_TARGET")
    ])

    report_df = base_df.join(
        ytd_values,
        on = base_grouping_keys,
        how = 'left'
    )

    #Monthly columns target and actual logic
    curr_month = curr_date.month
    curr_year = curr_date.year
    year_months = []
    for i in range(1,13):
        calc_month = curr_month - i
        calc_year = curr_year
        if calc_month <= 0:
            calc_month += 12
            calc_year -= 1
        yyyyMM = 100 * calc_year + calc_month
        year_months.append(yyyyMM)
        datetime_object = datetime.datetime(calc_year,calc_month,1)
        actual_column_name = f"{datetime_object.strftime(format = '%b_%y')} Acutual"
        target_column_name = f"{datetime_object.strftime(format = '%b_%y')} Target"
        
        month_values = combined_df.filter(
            pl.col("YEARMONTH") == yyyyMM
        ).group_by(base_grouping_keys).agg([
            pl.col('NETAMT').sum().alias(actual_column_name),
            pl.col('TARGET_NETAMT').sum().alias(target_column_name)
        ])

        report_df = report_df.join(
            month_values,
            on = base_grouping_keys,
            how = 'left'
        )

        report_df = report_df.with_columns(
            pl.col(actual_column_name).fill_null(0),
            pl.col(target_column_name).fill_null(0)
        )
    #12m rolling logic 
    twelve_month_rolling_values = combined_df.filter(
        pl.col('YEARMONTH').is_in(year_months)
    ).group_by(base_grouping_keys).agg([
        pl.col('NETAMT').sum().alias('12M_ROLLING_ACTUAL'),
        pl.col('TARGET_NETAMT').sum().alias('12M_ROLLING_TARGET')
    ])

    report_df = report_df.join(
        twelve_month_rolling_values,
        on = base_grouping_keys,
        how = 'left'
    )

    report_df = report_df.with_columns(
        pl.col("12M_ROLLING_ACTUAL").fill_null(0),
        pl.col("12M_ROLLING_TARGET").fill_null(0)
    )

    #yearly sums logic
    for i in range(1,3):
        calc_year = curr_year - i 
        year_values = combined_df.filter(
            pl.col('YEAR') == calc_year
        ).group_by(base_grouping_keys).agg([
            pl.col('NETAMT').sum().alias(f"{calc_year}_ACTUAL")
        ])
        report_df = report_df.join(
            year_values,
            on = base_grouping_keys,
            how = 'left'
        )
        report_df = report_df.with_columns(
            pl.col(f"{calc_year}_ACTUAL").fill_null(0)
        )
    
    report_three_combined = pl.DataFrame.to_pandas(report_df)
    report_three_combined.to_sql('REPORT_THREE_COMBINED',con = engine, schema = 'dbo', if_exists='replace', index=False)

if __name__ == "__main__":
    params = urllib.parse.quote_plus(SSMS_CONN_STRING)
    engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)
    """
    Begin by getting all INGRAM Sales data for the last 3 years not including the current month

    COLUMNS of TUTLIV.dbo.ING_SALES:
    ISBN    YEAR    MONTH   TITLE   NAMECUST    NETUNITS    NETAMT
    """
    ingram_sales_df = pl.from_pandas(pd.read_sql(ING_QUERY, engine)) #Query is in src/helpers/paths.py

    """
    Next, grab all SAGE Sales data for the last 3 years not including current month, also include no sales where namecust LIKE 'INGRAM BOOK CO.'

    COLUMNS of TUTLIV.dbo.ALL_HSA_MKSEG:
    NETAMT    NETUNITS     NEWBILLTO    ISBN    YEAR    MONTH   TITLE   NAMECUST    IDACCTSET 
    """
    sage_sales_df = pl.from_pandas(pd.read_sql(SAGE_QUERY, engine)) #Query is in src/helpers/paths.py
    """
    Get target calculations df reading from excel
    """
    target_calculations_df = pl.from_pandas(pd.read_excel(TARGET_CALCULATIONS_FILE,dtype = {'BILLTO':str}) )

    logger.info("Starting generation of COMBINED_SALES_REPORT")
    combined_sales_report(ingram_sales_df = ingram_sales_df,sage_sales_df = sage_sales_df)
    logger.info("Finished combining data and reporting logic for COMBINED_SALES_REPORT")
    logger.info("Starting REPORT_THREE_COMBINED")
    report_three_combined(ingram_sales_df = ingram_sales_df,sage_sales_df = sage_sales_df,target_calculations_df = target_calculations_df)
    logger.info("Finished REPORT_THREE_COMBINED")
    logger.info("Program has finished")
    time.sleep(3)
    sys.exit(0)
