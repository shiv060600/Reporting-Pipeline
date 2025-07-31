#type: ignore
import logging
import pandas as pd
import sqlalchemy
import urllib
import datetime
import json
from rapidfuzz import process, fuzz
import sys,os
import time


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.paths import PATHS

MONTHLY_ING_SALES = PATHS['MONTHLY_ING_SALES']
SSMS_CONN_STRING = PATHS['SSMS_CONN_STRING']

params = urllib.parse.quote_plus(SSMS_CONN_STRING)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

logging.basicConfig(
    filename="monthly_upload.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler('monthly_upload.log',mode='w')
    ]
)


try:
    monthly_sales_df = pd.read_excel(MONTHLY_ING_SALES, sheet_name='Sheet1')
    """
    Remove this filtering because we want to include all ingram sales
    #Only sales where IPS_Sale = 'N'
    monthly_sales_df = monthly_sales_df[monthly_sales_df['IPS_Sale'] == 'N']
    logging.info(f"Loaded {len(monthly_sales_df)} monthly sales records")
    """
except Exception as e:
    logging.error(f"Failed to read monthly sales file: {e}")
    sys.exit(1)

try:
    ingram_master_sales_categories = pd.read_sql(
        "SELECT TRIM([SL Account Number]) AS [SL Account Number], TRIM([HQ Account Number]) AS [HQ Account Number], TRIM([MASTER SALES CATEGORY]) AS [MASTER SALES CATEGORY] FROM TUTLIV.dbo.INGRAM_MASTER_CATEGORIES",
        engine
    )
    book_mapping = pd.read_sql("SELECT DISTINCT TRIM(Z_ID) as ISBN, TRIM(LONG_TITLE) as TITLE_Itemflat FROM TUTLIV.dbo.ITEMFLAT", engine)
    ingram_master_sales_categories = ingram_master_sales_categories.drop_duplicates(subset=['SL Account Number','HQ Account Number'])
    logging.info(f"Loaded {len(ingram_master_sales_categories)} master sales category records from SQL Server")
except Exception as e:
    logging.error(f"Failed to read master sales categories from SQL Server: {e}")
    sys.exit(1)

logging.info('Mapping master sales categories to TUTTLE SALES CATEGORY')

monthly_sales_df['SL Account Number'] = monthly_sales_df['SL Account Number'].astype(str).str.zfill(9)
ingram_master_sales_categories['SL Account Number'] = ingram_master_sales_categories['SL Account Number'].astype(str)

monthly_sales_df = monthly_sales_df.merge(ingram_master_sales_categories, how='left', on='SL Account Number')

logging.info('filling title nulls')
monthly_sales_df = monthly_sales_df.merge(book_mapping, left_on='EAN', right_on='ISBN', how='left')
monthly_sales_df['Title'] = monthly_sales_df['Title'].fillna(monthly_sales_df['TITLE_Itemflat'])
monthly_sales_df = monthly_sales_df.drop(columns=['TITLE_Itemflat'])

logging.info('removing extra spaces')
for column in monthly_sales_df.select_dtypes(include='object').columns:
    monthly_sales_df[column] = monthly_sales_df[column].str.strip()

null_count = monthly_sales_df['MASTER SALES CATEGORY'].isnull().sum()
if null_count > 0:
    logging.warning(f"Found {null_count} records with null MASTER SALES CATEGORY")

monthly_sales_df = monthly_sales_df.rename(columns={'MASTER SALES CATEGORY': 'TUTTLE SALES CATEGORY'})

logging.info('Finished loading and mapping data')

logging.info('Dropping unnecessary columns')
monthly_sales_df = monthly_sales_df.drop(
    columns=['SL ST', 'Free Units',
     'Return Credit', 'Gross Invc', 'Return Units', 
     'Sold Units', 'DC', 'SL City',
     'Shipping Location', 'Order Date'],
    errors='ignore' 
)

monthly_sales_df["YEAR"] = monthly_sales_df["Date"].str[6:]
monthly_sales_df["MONTH"] = monthly_sales_df["Date"].str[0:2].str.replace('0','')
logging.info('Added YEAR and MONTH columns')

monthly_sales_df = monthly_sales_df.rename(columns={
    'Net Sold Units': 'NETUNITS',
    'Net Invc' : 'NETAMT',
    'EAN': 'ISBN',
    'Title': 'TITLE',
    'Headquarter': 'NAMECUST',
})

# Remove 'TUTTLE SALES CATEGORY' from output columns and groupby
output_columns = ['NETUNITS','NETAMT', 'ISBN', 'YEAR', 'MONTH', 'TITLE', 'NAMECUST','SL Class of Trade','IPS Sale']
monthly_sales_df = monthly_sales_df[output_columns]

monthly_sales_df['NETUNITS'] = monthly_sales_df['NETUNITS'].fillna(0)
monthly_sales_df['NETAMT'] = monthly_sales_df['NETAMT'].fillna(0)
monthly_sales_df['NETUNITS'] = monthly_sales_df['NETUNITS'].round().astype(int)

def clean_isbn(isbn):
    if pd.isna(isbn):
        return None
    return str(isbn).replace('.0', '').replace('.', '').replace('E+', '').replace('e+', '')

monthly_sales_df['ISBN'] = monthly_sales_df['ISBN'].apply(clean_isbn)

invalid_isbns = monthly_sales_df[monthly_sales_df['ISBN'].str.len() != 13]
if not invalid_isbns.empty:
    logging.warning(f"Found {len(invalid_isbns)} ISBNs that are not 13 digits")

logging.info(f'Records before grouping: {monthly_sales_df.shape[0]}')
netunits_before = monthly_sales_df['NETUNITS'].sum()
netamt_before = monthly_sales_df['NETAMT'].sum()
logging.info(f"Total NETUNITS before: {netunits_before}")
logging.info(f"Total NETAMT before: {netamt_before}")

grouped = monthly_sales_df.groupby(['ISBN','YEAR','MONTH','TITLE','NAMECUST','IPS Sale']).agg({
    'NETUNITS':'sum',
    'NETAMT':'sum',
    'SL Class of Trade':'first'
}).reset_index()

logging.info('Grouping complete')

logging.info(f'Records after grouping: {grouped.shape[0]}')
netunits_after = grouped['NETUNITS'].sum()
netamt_after = grouped['NETAMT'].sum()
logging.debug(f"Total NETUNITS after grouping: {netunits_after}")
logging.debug(f"Total NETAMT after grouping: {netamt_after}")

logging.info("Starting monthly Ingram sales data append to SQL Server")
grouped.to_sql('ING_SALES', engine, index=False, if_exists='append', schema='dbo')

logging.info(f"Successfully appended {len(grouped)} rows to ING_SALES table")



