#type: ignore
import pandas as pd
import numpy as np
from dbfread import DBF
import sqlalchemy
import xlwings as xw
import urllib
from helpers.paths import PATHS
from helpers.paths import ING_QUERY, SAGE_QUERY
from combined_sales_report.combined_sales_report import combined_sales_report
from report_three_combined.report_three_combined import report_three_combined
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
        logging.FileHandler("logs_and_tests/reporting_pipeline.log", mode='w'),
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
    target_calculations_df = pl.from_pandas(pd.read_excel(TARGET_CALCULATIONS_FILE,sheet_name = 'Sheet1',dtype={
        "BILLTO": str,
        "COMPANY" : str,
        "2024" : float,
        "2025" : float,
        "MUL_RATIO" : float
    }))
    """
    logger.info("Starting generation of COMBINED_SALES_REPORT")
    combined_sales_report(ingram_sales_df = ingram_sales_df,sage_sales_df = sage_sales_df)
    logger.info("Finished combining data and reporting logic for COMBINED_SALES_REPORT")
    logger.info("Starting REPORT_THREE_COMBINED")
    """
    report_three_combined(ingram_sales_df = ingram_sales_df,sage_sales_df = sage_sales_df,target_calculations_df = target_calculations_df)
    logger.info("Finished REPORT_THREE_COMBINED")
    logger.info("Program has finished")
    time.sleep(3)
    sys.exit(0)
