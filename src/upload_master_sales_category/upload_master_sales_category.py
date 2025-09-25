#type: ignore
import pandas as pd
import sqlalchemy
import xlwings as xw
import urllib,os,sys,logging

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.paths import PATHS

MASTER_SALES_CATEGORIES = PATHS["MASTER_SALES_CATEGORIES"]
SSMS_CONN_STRING = PATHS["SSMS_CONN_STRING"]
params = urllib.parse.quote_plus(SSMS_CONN_STRING)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs_and_tests/upload_master_sales.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


def main():
    try:
        app = xw.App(visible = False)
        logging.info(f"Opening Excel file: {MASTER_SALES_CATEGORIES}")
        wb = app.books.open(MASTER_SALES_CATEGORIES)
        
        logging.info("Accessing INGRAM_CUSTOMERS sheet")
        ws = wb.sheets["INGRAM_CUSTOMERS"]
        
        logging.info("Reading data from Excel sheet")
        last_row = ws.range('A1').end('down').row
        ingram_master_sales_categories: pd.DataFrame = ws.range(f'A1:E{last_row}').options(pd.DataFrame, header=True, index=False, dtype=str).value
        
        logging.info(f"Successfully loaded {len(ingram_master_sales_categories)} rows from Excel")
        
        
        wb.save()
        wb.close()
        logging.info('Successfully loaded ingram categories from Excel')
        
    except Exception as e:
        logging.error(f"Failed to load ingram categories: {e}")
        if 'wb' in locals():
            try:
                wb.close()
            except:
                pass
        if app is not None:
            app.quit()
        sys.exit(1)
    finally:
        app.quit()

    try:
        app1 = xw.App(visible = False)
        wb = app1.books.open(MASTER_SALES_CATEGORIES)
        ws = wb.sheets['SAGE_CUSTOMERS']
        last_row = ws.range('A1').end('down').row
        logging.info("Loading SAGE categories")
        sage_master_categories : pd.DataFrame = ws.range(f"A1:D{last_row}").options(pd.DataFrame, header=True, index=False, dtype=str).value
            
        logging.info("Successfully loaded SAGE categories")
    except Exception as e:
        logging.error(f"Failed to load SAGE categories {e}")
        if 'wb' in locals():
            try:
                wb.close()
            except Exception as e:
                logging.info(f"Faled to close wb during SAGE categories trycatch : {e}")
                pass
        if app1 is not None:
            app1.quit()
        sys.exit(1)
    finally:
        app1.quit()

    # Upload to SQL Server
    try:
        logging.info("Uploading data to SQL Server...")
        ingram_master_sales_categories.to_sql('INGRAM_MASTER_CATEGORIES', engine, index=False, if_exists='replace', schema='dbo')
        logging.info(f'Successfully uploaded {len(ingram_master_sales_categories)} rows to SQL Server table INGRAM_MASTER_CATEGORIES')
        sage_master_categories.to_sql("SAGE_MASTER_CATEGORIES",engine,index=False,if_exists='replace',schema='dbo')
        logging.info(f"Successfully uploaded {len(sage_master_categories)} rows to SQL Server table SAGE_MASTER_CATEOGRIES")
    except Exception as e:
        logging.error(f"Failed to upload to SQL Server: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    logging.info('starting upload_master_sales_categories')
    main()
    logging.info('finished upload_master_sales_categories')