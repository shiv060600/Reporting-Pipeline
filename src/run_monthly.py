#type: ignore
import pandas as pd
import numpy as np
from dbfread import DBF
import sqlalchemy
from sqlalchemy.engine import Engine
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
from database_uploads.monthly_sales_upload_ing import monthly_sales_upload

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs_and_tests/monthly_sales_upload_automatic.log", mode='w'),
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
This Code will run monthly, 
It will upload the new monthly sales on the first of every month.
it will be run on the first of every month at 6pm. 
the daily pipline will then run at 9pm.
"""

if __name__ == "__main__":
    logger.info('Beginning automatic monthly sales upload')
    monthly_sales_upload()
    logger.info('Finished autmoatic monthly sales upload')

    
