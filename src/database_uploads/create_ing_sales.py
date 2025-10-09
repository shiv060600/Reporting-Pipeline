# pyright: ignore[reportMissingImports]
# type: ignore
import urllib.parse
import pandas as pd 
import sqlalchemy

import xlwings as xw
import urllib
import openpyxl
import pyodbc
import logging
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

"""
This program exists to replace the ING_SALES table in TUTLIV,
It will replace it with the data in whatever excel file is at the location: H:\\Upgrading_Database_Reporting_Systems\\Resources\\ing_sales_data.xlsx
"""

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.paths import PATHS

SSMS_CONN_STRING = PATHS["SSMS_CONN_STRING"]
ING_SALES_PATH = PATHS["HISTORICAL_ING_SALES"]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs_and_tests/create_ing_sales_log.txt"),
        logging.StreamHandler(sys.stdout)
    ]
)

params = urllib.parse.quote_plus(SSMS_CONN_STRING)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

logging.info('Loading historical files from Excel')

try:
    ing_sales_df = pd.read_excel(ING_SALES_PATH, sheet_name='Sheet1',dtype={'SL Account Number': str, 'HQ Account Number': str})
    logging.info(f"Loaded {len(ing_sales_df)} records from Excel")
except Exception as e:
    logging.error(f"Failed to read Excel file: {e}")
    sys.exit(1)

"""
Remove this so we have all sales and filter later on.
# Filter for IPS Sale = 'N' only
ing_sales_df = ing_sales_df[ing_sales_df['IPS Sale'] == 'N']
logging.info(f"After filtering IPS Sale = 'N': {len(ing_sales_df)} records")
"""

try:
    ingram_master_sales_categories = pd.read_sql(
        "SELECT TRIM([SL Account Number]) AS [SL Account Number], TRIM([HQ Account Number]) AS [HQ Account Number], TRIM([MASTER SALES CATEGORY]) AS [MASTER SALES CATEGORY] FROM TUTLIV.dbo.INGRAM_MASTER_CATEGORIES",
        engine
    )
    book_mapping = pd.read_sql("SELECT DISTINCT TRIM(Z_ID) as ISBN, TRIM(LONG_TITLE) as TITLE_Itemflat FROM TUTLIV.dbo.ITEMFLAT",engine)
    ingram_master_sales_categories = ingram_master_sales_categories.drop_duplicates(subset=['SL Account Number','HQ Account Number'])
    logging.info(f"Loaded {len(ingram_master_sales_categories)} master sales category records from SQL Server")
except Exception as e:
    logging.error(f"Failed to read master sales categories from SQL Server: {e}")
    sys.exit(1)

logging.info('Mapping master sales categories to TUTTLE SALES CATEGORY')


ingram_master_sales_categories['SL Account Number'] = ingram_master_sales_categories['SL Account Number'].astype(str)
ing_sales_df['SL Account Number'] = ing_sales_df['SL Account Number'].astype(str)
ingram_master_sales_categories['HQ Account Number'] = ingram_master_sales_categories['HQ Account Number'].astype(str)
ing_sales_df['HQ Account Number'] = ing_sales_df['HQ Account Number'].astype(str)

ing_sales_df = ing_sales_df.merge(ingram_master_sales_categories, how='left', on=['SL Account Number','HQ Account Number'])

logging.info('removing extra spaces')
for column in ing_sales_df.select_dtypes(include='object').columns:
    ing_sales_df[column] = ing_sales_df[column].str.strip()

logging.info('filling title nulls')
ing_sales_df['EAN'] = ing_sales_df['EAN'].astype(str)
book_mapping['ISBN'] = book_mapping['ISBN'].astype(str)
ing_sales_df = ing_sales_df.merge(book_mapping,left_on='EAN',right_on='ISBN', how='left')
ing_sales_df['Title'] = ing_sales_df['Title'].fillna(ing_sales_df['TITLE_Itemflat'])
ing_sales_df = ing_sales_df.drop(columns=['TITLE_Itemflat'])



null_count = ing_sales_df['MASTER SALES CATEGORY'].isnull().sum()
if null_count > 0:
    logging.warning(f"Found {null_count} records with null MASTER SALES CATEGORY")

ing_sales_df = ing_sales_df.rename(columns={'MASTER SALES CATEGORY': 'TUTTLE SALES CATEGORY'})


logging.info('Processing data for SQL upload')
ing_sales_df = ing_sales_df.drop(
    columns=['SL ST', 'Free Units', 'Return Credit', 'Gross Invc', 'Return Units', 
             'Sold Units', 'DC', 'SL City', 'Shipping Location', 'Order Date','ISBN'],
    errors='ignore' 
)

ing_sales_df["YEAR"] = ing_sales_df["Date"].str[6:]
ing_sales_df["MONTH"] = ing_sales_df["Date"].str[0:2].str.replace('0','')

ing_sales_df = ing_sales_df.rename(columns={
    'Net Sold Units': 'NETUNITS',
    'Net Invc' : 'NETAMT',
    'EAN': 'ISBN',
    'Title': 'TITLE',
    'Headquarter': 'NAMECUST',
})

# Remove 'TUTTLE SALES CATEGORY' from output columns and groupby
output_columns = ['NETUNITS','NETAMT', 'ISBN', 'YEAR', 'MONTH', 'TITLE', 'NAMECUST',
                 'SL Class of Trade','HQ Account Number','SL Account Number','IPS Sale']
ing_sales_df = ing_sales_df[output_columns]

ing_sales_df['NETUNITS'] = ing_sales_df['NETUNITS'].fillna(0)
ing_sales_df['NETAMT'] = ing_sales_df['NETAMT'].fillna(0)
ing_sales_df['NETUNITS'] = ing_sales_df['NETUNITS'].round().astype(int)

def clean_isbn(isbn):
    if pd.isna(isbn):
        return None
    return str(isbn).replace('.0', '').replace('.', '').replace('E+', '').replace('e+', '')

ing_sales_df['ISBN'] = ing_sales_df['ISBN'].apply(clean_isbn)

invalid_isbns = ing_sales_df[ing_sales_df['ISBN'].str.len() != 13]
if not invalid_isbns.empty:
    logging.warning(f"Found {len(invalid_isbns)} ISBNs that are not 13 digits")

logging.info(f'Records before grouping: {ing_sales_df.shape[0]}')
netunits_before = ing_sales_df['NETUNITS'].sum()
netamt_before = ing_sales_df['NETAMT'].sum()
logging.info(f"Total NETUNITS before: {netunits_before}")
logging.info(f"Total NETAMT before: {netamt_before}")

logging.info(f"Rows with NETUNITS < 0 before grouping: {len(ing_sales_df[ing_sales_df['NETUNITS'] < 0])}")
logging.info(f"Sum of NETUNITS < 0 before grouping: {ing_sales_df[ing_sales_df['NETUNITS'] < 0]['NETUNITS'].sum()}")
logging.info(f"Rows with NETUNITS == 0 before grouping: {len(ing_sales_df[ing_sales_df['NETUNITS'] == 0])}")
logging.info(f"Sum of NETUNITS == 0 before grouping: {ing_sales_df[ing_sales_df['NETUNITS'] == 0]['NETUNITS'].sum()}")
logging.info(f"Rows with NETAMT < 0 before grouping: {len(ing_sales_df[ing_sales_df['NETAMT'] < 0])}")
logging.info(f"Sum of NETAMT < 0 before grouping: {ing_sales_df[ing_sales_df['NETAMT'] < 0]['NETAMT'].sum()}")
logging.info(f"Rows with NETAMT == 0 before grouping: {len(ing_sales_df[ing_sales_df['NETAMT'] == 0])}")
logging.info(f"Sum of NETAMT == 0 before grouping: {ing_sales_df[ing_sales_df['NETAMT'] == 0]['NETAMT'].sum()}")

grouped = ing_sales_df.groupby(['ISBN','YEAR','MONTH','TITLE','NAMECUST','HQ Account Number','SL Account Number','IPS Sale']).agg({
    'NETUNITS':'sum',
    'NETAMT':'sum',
    'SL Class of Trade':'first'
}).reset_index()

logging.info(f"Groups with NETUNITS < 0 after grouping: {len(grouped[grouped['NETUNITS'] < 0])}")
logging.info(f"Sum of NETUNITS < 0 after grouping: {grouped[grouped['NETUNITS'] < 0]['NETUNITS'].sum()}")
logging.info(f"Groups with NETUNITS == 0 after grouping: {len(grouped[grouped['NETUNITS'] == 0])}")
logging.info(f"Sum of NETUNITS == 0 after grouping: {grouped[grouped['NETUNITS'] == 0]['NETUNITS'].sum()}")
logging.info(f"Groups with NETAMT < 0 after grouping: {len(grouped[grouped['NETAMT'] < 0])}")
logging.info(f"Sum of NETAMT < 0 after grouping: {grouped[grouped['NETAMT'] < 0]['NETAMT'].sum()}")
logging.info(f"Groups with NETAMT == 0 after grouping: {len(grouped[grouped['NETAMT'] == 0])}")
logging.info(f"Sum of NETAMT == 0 after grouping: {grouped[grouped['NETAMT'] == 0]['NETAMT'].sum()}")
logging.info(f"Groups with NETAMT > 0 after grouping: {len(grouped[grouped['NETAMT'] > 0])}")
logging.info(f"Sum of NETAMT > 0 after grouping: {grouped[grouped['NETAMT'] > 0]['NETAMT'].sum()}")

logging.info(f"Sample groups with NETUNITS < 0 after grouping:\n{grouped[grouped['NETUNITS'] < 0].head(5)}")
logging.info(f"Sample groups with NETUNITS == 0 after grouping:\n{grouped[grouped['NETUNITS'] == 0].head(5)}")
logging.info(f"Sample groups with NETAMT < 0 after grouping:\n{grouped[grouped['NETAMT'] < 0].head(5)}")
logging.info(f"Sample groups with NETAMT == 0 after grouping:\n{grouped[grouped['NETAMT'] == 0].head(5)}")

# Use grouped for further processing
ing_sales_df = grouped

logging.info(f'Records after grouping: {ing_sales_df.shape[0]}')
netunits_after = ing_sales_df['NETUNITS'].sum()
netamt_after = ing_sales_df['NETAMT'].sum()
logging.info(f"Total NETUNITS after: {netunits_after}")
logging.info(f"Total NETAMT after: {netamt_after}")

logging.info("Creating SQL Server table with the processed data")
ing_sales_df.to_sql('ING_SALES', engine, index=False, if_exists='replace', schema='dbo')
logging.info(f"Successfully created ING_SALES table with {len(ing_sales_df)} records")
