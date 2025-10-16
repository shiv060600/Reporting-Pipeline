#type: ignore
import sys,os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
import pandas as pd
import sqlalchemy
import urllib
import datetime
import json
from rapidfuzz import process, fuzz
import time
from helpers.paths import PATHS
import datetime


logger = logging.getLogger(__name__)


MONTHLY_ING_SALES = PATHS['MONTHLY_ING_SALES']
SSMS_CONN_STRING = PATHS['SSMS_CONN_STRING']


def monthly_sales_upload() -> None:
    params = urllib.parse.quote_plus(SSMS_CONN_STRING)
    engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

    curr_date = datetime.datetime.now()
    curr_month = curr_date.month

    if curr_month == 1:
        prev_month = 12
    else:
        prev_month = curr_month - 1


    try:
        monthly_sales_df = pd.read_excel(MONTHLY_ING_SALES, sheet_name='Sheet1',dtype={'SL Account Number': str, 'HQ Account Number': str, 'EAN': str})
        """
        Remove this filtering because we want to include all ingram sales
        #Only sales where IPS_Sale = 'N'
        monthly_sales_df = monthly_sales_df[monthly_sales_df['IPS_Sale'] == 'N']
        logger.info(f"Loaded {len(monthly_sales_df)} monthly sales records")
        """
        logger.info(f"columns in monthly sales: {monthly_sales_df.columns.tolist()}")
    except Exception as e:
        logger.error(f"Failed to read monthly sales file: {e}")
        sys.exit(1)
    

    try:
        ingram_master_sales_categories = pd.read_sql(
            "SELECT TRIM([SL Account Number]) AS [SL Account Number], TRIM([HQ Account Number]) AS [HQ Account Number], TRIM([MASTER SALES CATEGORY]) AS [MASTER SALES CATEGORY] FROM TUTLIV.dbo.INGRAM_MASTER_CATEGORIES",
            engine
        )
        book_mapping = pd.read_sql("SELECT DISTINCT TRIM(Z_ID) as ISBN, TRIM(LONG_TITLE) as TITLE_Itemflat FROM TUTLIV.dbo.ITEMFLAT", engine)
        ingram_master_sales_categories = ingram_master_sales_categories.drop_duplicates(subset=['SL Account Number','HQ Account Number'])
        logger.info(f"Loaded {len(ingram_master_sales_categories)} master sales category records from SQL Server")
    except Exception as e:
        logger.error(f"Failed to read master sales categories from SQL Server: {e}")
        sys.exit(1)

    logger.info('Mapping master sales categories to TUTTLE SALES CATEGORY')

    ingram_master_sales_categories['SL Account Number'] = ingram_master_sales_categories['SL Account Number'].astype(str)
    ingram_master_sales_categories['HQ Account Number'] = ingram_master_sales_categories['HQ Account Number'].astype(str)

    monthly_sales_df = monthly_sales_df.merge(ingram_master_sales_categories, how='left', on=['SL Account Number','HQ Account Number'])

    logger.info('filling title nulls')
    monthly_sales_df = monthly_sales_df.merge(book_mapping, left_on='EAN', right_on='ISBN', how='left')
    monthly_sales_df['Title'] = monthly_sales_df['Title'].fillna(monthly_sales_df['TITLE_Itemflat'])
    monthly_sales_df = monthly_sales_df.drop(columns=['TITLE_Itemflat', 'ISBN'])
    logger.info('removing extra spaces')
    for column in monthly_sales_df.select_dtypes(include='object').columns:
        monthly_sales_df[column] = monthly_sales_df[column].str.strip()

    null_count = monthly_sales_df['MASTER SALES CATEGORY'].isnull().sum()
    if null_count > 0:
        logger.warning(f"Found {null_count} records with null MASTER SALES CATEGORY")

    monthly_sales_df = monthly_sales_df.rename(columns={'MASTER SALES CATEGORY': 'TUTTLE SALES CATEGORY'})

    logger.info('Finished loading and mapping data')

    logger.info('Dropping unnecessary columns')
    monthly_sales_df = monthly_sales_df.drop(
        columns=['SL ST', 'Free Units',
        'Return Credit', 'Gross Invc', 'Return Units', 
        'Sold Units', 'DC', 'SL City',
        'Shipping Location', 'Order Date'],
        errors='ignore' 
    )


    monthly_sales_df["Date"] = pd.to_datetime(monthly_sales_df["Date"], format='%m/%d/%Y', errors='coerce')

    monthly_sales_df["YEAR"] = monthly_sales_df['Date'].dt.year
    monthly_sales_df["MONTH"] = monthly_sales_df["Date"].dt.month
    
    

    logger.info('Added YEAR and MONTH columns')

    monthly_sales_df = monthly_sales_df.rename(columns={
        'Net Sold Units': 'NETUNITS',
        'Net Invc' : 'NETAMT',
        'EAN': 'ISBN',
        'Title': 'TITLE',
        'Headquarter': 'NAMECUST',
    })

    # Remove 'TUTTLE SALES CATEGORY' from output columns and groupby
    output_columns = ['NETUNITS','NETAMT', 'ISBN', 'YEAR', 'MONTH', 'TITLE', 'NAMECUST','SL Class of Trade','IPS Sale','HQ Account Number','SL Account Number']
    monthly_sales_df = monthly_sales_df[output_columns]

    monthly_sales_df['NETUNITS'] = monthly_sales_df['NETUNITS'].fillna(0)
    monthly_sales_df['NETAMT'] = monthly_sales_df['NETAMT'].fillna(0)
    monthly_sales_df['NETUNITS'] = monthly_sales_df['NETUNITS'].round().astype(int)
    monthly_sales_df['ISBN'] = monthly_sales_df['ISBN'].astype(str)


    invalid_isbns = monthly_sales_df[monthly_sales_df['ISBN'].str.len() != 13]
    if not invalid_isbns.empty:
        logger.warning(f"Found {len(invalid_isbns)} ISBNs that are not 13 digits")

    logger.info(f'Records before grouping: {monthly_sales_df.shape[0]}')
    netunits_before = monthly_sales_df['NETUNITS'].sum()
    netamt_before = monthly_sales_df['NETAMT'].sum()
    logger.info(f"Total NETUNITS before: {netunits_before}")
    logger.info(f"Total NETAMT before: {netamt_before}")

    grouped = monthly_sales_df.groupby(['ISBN','YEAR','MONTH','TITLE','NAMECUST','IPS Sale','HQ Account Number','SL Account Number']).agg({
        'NETUNITS':'sum',
        'NETAMT':'sum',
        'SL Class of Trade':'first'
    }).reset_index()

    logger.info('Grouping complete')

    logger.info(f'Records after grouping: {grouped.shape[0]}')
    netunits_after = grouped['NETUNITS'].sum()
    netamt_after = grouped['NETAMT'].sum()
    logger.debug(f"Total NETUNITS after grouping: {netunits_after}")
    logger.debug(f"Total NETAMT after grouping: {netamt_after}")

    logger.info("Starting monthly Ingram sales data append to SQL Server")
    grouped.to_sql('ING_SALES', engine, index=False, if_exists='append', schema='dbo')

    logger.info(f"Successfully appended {len(grouped)} rows to ING_SALES table")

if __name__ == "__main__":
    logging.basicConfig(
        level = logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(r'H:\Upgrading_Database_Reporting_Systems\REPORTING_PIPELINE\src\logs_and_tests\manual_run_monthly_sales_upload.log')
        ]

    )
    logger.info('Beginning manual execution of monthtly_sales_upload')
    monthly_sales_upload()
    logger.info('finished monthly sales upload')





