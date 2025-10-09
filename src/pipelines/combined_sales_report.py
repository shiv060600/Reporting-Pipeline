#type: ignore
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
import time
import sqlite3
import polars as pl
import logging
from sqlalchemy.engine import Engine

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


def combined_sales_report(ingram_sales_df:pl.DataFrame,sage_sales_df:pl.DataFrame, tutliv_engine: Engine):

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
        tutliv_engine
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
            tutliv_engine
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
            con=tutliv_engine,
            schema=schema,
            if_exists='replace',
            index=False
        )
        logger.info(f"Successfully exported {len(pandas_df)} rows to {schema}.{production_table_name}")
        
    except Exception as e:
        logger.error(f"Error uploading to SQL Server test table: {e}")
    finally:
        #engine disposal handled in main.py
        pass
        
    logger.info("Processing completed successfully")