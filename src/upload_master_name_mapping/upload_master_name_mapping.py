#type: ignore
import pandas as pd
import xlwings as xw
import sqlalchemy
import openpyxl
import pyodbc
import logging
import os,urllib,sys
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.paths import PATHS

logging.basicConfig(
    level = logging.INFO,
    handlers = [
        logging.FileHandler('logs_and_tests/upload_master_name_mapping.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

SSMS_CONN_STRING = PATHS["SSMS_CONN_STRING"]
MASTER_NAME_MAPPING_FILE = PATHS["MASTER_NAME_MAPPING_FILE"]

params = urllib.parse.quote_plus(SSMS_CONN_STRING)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)


def main():
    try:
        app = xw.App(visible = False)
        wb = app.books.open(MASTER_NAME_MAPPING_FILE)
        ws = wb.sheets['INGRAM_NAMES']
        last_row = ws.range("A1").end('down').row
        logging.info("Starting load")
        name_mapping_df = ws.range(f"A1:I{last_row}").options(pd.DataFrame,header=True,index = False).value
        logging.info(f"Loaded columns: {list(name_mapping_df.columns)}")
        wb.close()
        logging.info("Successfully loaded name mapping data from Excel")
    except Exception as e:
        logging.info(f"failedto load name mapping {e}")
    finally:
        if app is not None:
            try:
                logging.info("Closing Excel application")
                app.quit()
                logging.info("Excel application closed successfully")
            except Exception as e:
                logging.warning(f"Error closing Excel app: {e}")


    #Upload

    if not name_mapping_df.empty:
        try:
            name_mapping_df.to_sql(
                'MASTER_INGRAM_NAME_MAPPING', 
                engine, 
                schema='dbo', 
                index=False, 
                if_exists='replace'
            )
            logging.info(f"successfully uploaded {len(name_mapping_df)} rows to SQL Server Table: MASTER_INGRAM_NAME_MAPPING")
        except Exception as e:
            logging.info(f"failed to upload to SQL server {e}")
    else:
        logging.info('name_mapping_df is None')

if __name__ == "__main__":
    logging.info('starting upload_master_name_mapping.py')
    main()
    logging.info('successfully finished upload_master_name_mapping.py')






