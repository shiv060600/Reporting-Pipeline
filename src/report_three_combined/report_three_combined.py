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
        logging.FileHandler("logs_and_tests/reporting_pipeline.log",mode='w'),
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


def report_three_combined(ingram_sales_df: pl.DataFrame,sage_sales_df: pl.DataFrame,target_calculations_df: pl.DataFrame):
    #Order and standardize data (need IDs for mapping multiplication)
    column_order_ing = ['HQ_NUMBER','SL_NUMBER','ISBN', 'YEAR', 'MONTH', 'TITLE', 'NAMECUST', 'NETUNITS', 'NETAMT', 'TUTTLE_SALES_CATEGORY']
    column_order_sage = ['SAGE_ID','ISBN', 'YEAR', 'MONTH', 'TITLE', 'NAMECUST', 'NETUNITS', 'NETAMT', 'TUTTLE_SALES_CATEGORY']
    column_order_target_calculations  = ['BILLTO','MUL_RATIO','2025']
    ingram_sales_df = ingram_sales_df.select(column_order_ing)
    sage_sales_df = sage_sales_df.select(column_order_sage)
    target_calculations_df = target_calculations_df.select(column_order_target_calculations)

    target_calculations_df =  target_calculations_df.drop_nulls(subset=['BILLTO','MUL_RATIO'])

    target_calculations_df = target_calculations_df.filter(
        (pl.col("BILLTO") != '') & (pl.col("MUL_RATIO") != 0.0)
    )

    target_calculations_df = target_calculations_df.with_columns(
        pl.col("BILLTO").cast(pl.Utf8).str.strip_chars(), 
        pl.col("MUL_RATIO").cast(pl.Float64),
        pl.col("2025").cast(pl.Float64).alias('2025_Target')
    )

    ingram_sales_df = ingram_sales_df.with_columns(
        pl.col("HQ_NUMBER").cast(pl.Utf8).str.strip_chars(),
        pl.col("SL_NUMBER").cast(pl.Utf8).str.strip_chars(),
        pl.col("ISBN").cast(pl.Utf8).str.strip_chars(),
        pl.col("YEAR").cast(pl.Int64),
        pl.col("MONTH").cast(pl.Int64),
        pl.col("TITLE").cast(pl.Utf8).str.strip_chars(),
        pl.col("NAMECUST").cast(pl.Utf8).str.strip_chars(),
        pl.col("NETUNITS").cast(pl.Int64),
        pl.col("NETAMT").cast(pl.Int64),
        pl.col("TUTTLE_SALES_CATEGORY").cast(pl.Utf8).str.strip_chars()
    ).with_columns(
        (pl.col("YEAR") * 100 + pl.col("MONTH")).alias("YEARMONTH")
    ).drop(['YEAR','MONTH'])

    sage_sales_df = sage_sales_df.with_columns(
        pl.col("SAGE_ID").cast(pl.Utf8).str.strip_chars(),
        pl.col("ISBN").cast(pl.Utf8).str.strip_chars(),
        pl.col("YEAR").cast(pl.Int64),  
        pl.col("MONTH").cast(pl.Int64),
        pl.col("TITLE").cast(pl.Utf8).str.strip_chars(),
        pl.col("NAMECUST").cast(pl.Utf8).str.strip_chars(),
        pl.col("NETUNITS").cast(pl.Int64),
        pl.col("NETAMT").cast(pl.Int64),
        pl.col("TUTTLE_SALES_CATEGORY").cast(pl.Utf8).str.strip_chars()
    ).with_columns(
        (pl.col("YEAR") * 100 + pl.col("MONTH")).alias("YEARMONTH")
    ).drop(['YEAR','MONTH'])

    ingram_sales_df = ingram_sales_df.join(
        target_calculations_df,
        left_on = 'HQ_NUMBER',
        right_on = 'BILLTO',
        how = 'left'
    ).with_columns(
        (pl.col("NETAMT") * pl.col("MUL_RATIO").fill_null(1.0)).cast(pl.Float64).alias("TARGET_NETAMT")
    )


    sage_sales_df = sage_sales_df.join(
        target_calculations_df,
        left_on = 'SAGE_ID',
        right_on = 'BILLTO',
        how = 'left'
    ).with_columns(
        (pl.col('NETAMT') * pl.col('MUL_RATIO').fill_null(1.0)).cast(pl.Float64).alias("TARGET_NETAMT")
    )

    #Drop MUL_RATIO after chaining operations
    ingram_sales_df = ingram_sales_df.drop(['MUL_RATIO'])
    sage_sales_df = sage_sales_df.drop(['MUL_RATIO'])

    #drop ID columns for concant
    ingram_sales_df = ingram_sales_df.drop(["HQ_NUMBER","SL_NUMBER","ISBN","TITLE"])
    sage_sales_df = sage_sales_df.drop(["SAGE_ID","ISBN","TITLE"])

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
    grouping_keys = ["NAMECUST","TUTTLE_SALES_CATEGORY","YEARMONTH"]
    aggregate_expressions = [
        pl.col("NETAMT").sum().alias("NETAMT"),
        pl.col("NETUNITS").sum().alias("NETUNITS"),
        pl.col("TARGET_NETAMT").sum().alias("TARGET_NETAMT"),
        pl.col("2025_Target").first().alias('2025_Target')
    ]

    combined_df = combined_df.group_by(grouping_keys).agg(aggregate_expressions)
    base_df = combined_df.select(['NAMECUST','TUTTLE_SALES_CATEGORY','2025_Target']).unique()
    curr_date = datetime.datetime.now()

    ytd_values = combined_df.filter(
        pl.col("YEARMONTH").cast(pl.Utf8).str.slice(0,4).cast(pl.Int64) == curr_date.year
    ).group_by(['NAMECUST', 'TUTTLE_SALES_CATEGORY']).agg([
        pl.col("NETAMT").sum().alias("YTD_ACTUAL"),
        pl.col("TARGET_NETAMT").sum().alias("YTD_TARGET")
    ])

    report_df = base_df.join(
        ytd_values,
        on=['NAMECUST', 'TUTTLE_SALES_CATEGORY'],
        how='left'
    )

    report_df = report_df.with_columns(
        pl.col("2025_Target").fill_null(404.404)
    )

    #Monthly columns target and actual logic
    curr_month = curr_date.month
    curr_year = curr_date.year
    year_months = []
    target_months_to_drop = []
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

        #add to drop list if not the previous month
        if i != 1:
            target_months_to_drop.append(target_column_name)
        
        month_values = combined_df.filter(
            pl.col("YEARMONTH") == yyyyMM
        ).group_by(['NAMECUST', 'TUTTLE_SALES_CATEGORY']).agg([
            pl.col('NETAMT').sum().alias(actual_column_name),
            pl.col('TARGET_NETAMT').sum().alias(target_column_name)
        ])

        report_df = report_df.join(
            month_values,
            on=['NAMECUST', 'TUTTLE_SALES_CATEGORY'],
            how='left'
        )

        report_df = report_df.with_columns(
            pl.col(actual_column_name).fill_null(0),
            pl.col(target_column_name).fill_null(0)
        )
    #12m rolling logic 
    twelve_month_rolling_values = combined_df.filter(
        pl.col('YEARMONTH').is_in(year_months)
    ).group_by(['NAMECUST', 'TUTTLE_SALES_CATEGORY']).agg([
        pl.col('NETAMT').sum().alias('12M_ROLLING_ACTUAL'),
        pl.col('TARGET_NETAMT').sum().alias('12M_ROLLING_TARGET')
    ])

    report_df = report_df.join(
        twelve_month_rolling_values,
        on=['NAMECUST', 'TUTTLE_SALES_CATEGORY'],
        how='left'
    )

    report_df = report_df.with_columns(
        pl.col("12M_ROLLING_ACTUAL").fill_null(0),
        pl.col("12M_ROLLING_TARGET").fill_null(0)
    )

    #yearly sums logic
    for i in range(1,3):
        calc_year = curr_year - i 
        year_values = combined_df.filter(
            pl.col('YEARMONTH').cast(pl.Utf8).str.slice(0,4).cast(pl.Int64) == calc_year
        ).group_by(['NAMECUST', 'TUTTLE_SALES_CATEGORY']).agg([
            pl.col('NETAMT').sum().alias(f"{calc_year}_ACTUAL")
        ])
        report_df = report_df.join(
            year_values,
            on=['NAMECUST', 'TUTTLE_SALES_CATEGORY'],
            how='left'
        )
        report_df = report_df.with_columns(
            pl.col(f"{calc_year}_ACTUAL").fill_null(0)
        )
    
    #drop columns 
    cols_to_drop = target_months_to_drop
    report_df = report_df.drop(cols_to_drop)

    #rename some columns
    report_df = report_df.rename({
        "TUTTLE_SALES_CATEGORY" : "Category",
        "NAMECUST" : "Customer"
    })

    
    report_df = report_df.with_columns(
        (((pl.col("YTD_TARGET") / pl.col("2025_Target"))*100).round(1)).alias('Capture_Rate'),
        (pl.col("2025_Target") - pl.col("YTD_ACTUAL")).alias('Target_Remaining')
    )

    # Get current month/year for dynamic column identification
    curr_month_str = datetime.datetime(curr_year, curr_month - 1 if curr_month > 1 else 12, 1).strftime('%b_%y')
    current_target_col = f"{curr_month_str} Target"

    # Identify month columns (actuals only, excluding targets except current month)
    month_actual_columns = [col for col in report_df.columns if ' Acutual' in col]
    month_target_columns = [col for col in report_df.columns if ' Target' in col and col == current_target_col]

    # Sort month columns chronologically (most recent first)
    def sort_month_columns(col_list):
        def month_sort_key(col):
            # Extract month and year from column name
            parts = col.replace(' Acutual', '').replace(' Target', '').split('_')
            month_abbr, year_str = parts[0], parts[1]
            month_num = datetime.datetime.strptime(month_abbr, '%b').month
            year_num = 2000 + int(year_str)  # Convert 2-digit year to 4-digit
            return (year_num, month_num)
        return sorted(col_list, key=month_sort_key, reverse=True)

    sorted_month_actuals = sort_month_columns(month_actual_columns)

    # Get yearly columns
    yearly_columns = [col for col in report_df.columns if col.endswith('_ACTUAL') and col not in ['YTD_ACTUAL', '12M_ROLLING_ACTUAL']]
    yearly_columns = sorted(yearly_columns, reverse=True)  # Most recent year first


    # Build final column order
    final_column_order = [
        'Category',
        'Customer',
        current_target_col,  # Aug-25 Target (current month target)
        sorted_month_actuals[0] if sorted_month_actuals else '',  # Aug-25 Actual (most recent month)
    ] + sorted_month_actuals[1:] + [
        '12M_ROLLING_ACTUAL',
        '2025_Target',
        'YTD_TARGET', 
        'YTD_ACTUAL',
        'Capture_Rate', 
        'Target_Remaining',  
    ] + yearly_columns
    # Filter to only include columns that actually exist in the dataframe
    final_columns = [col for col in final_column_order if col in report_df.columns and col != '']

    # Reorder the dataframe
    report_df = report_df.select(final_columns)

    #sort
    report_df = report_df.sort('12M_ROLLING_ACTUAL',descending = True)

    #cast int
    for col in final_columns:
        if col == 'Capture_Rate':
            continue
        if report_df[col].dtype in [pl.Float32,pl.Float64,pl.Int64,pl.Int32]:
            report_df = report_df.with_columns(
                pl.col(col).fill_null(404.404).cast(pl.Int64)
            )

    
    #for testing
    report_df.write_csv("logs_and_tests/report_3_combined.csv")