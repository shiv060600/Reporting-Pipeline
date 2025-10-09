#type: ignore
import pandas as pd
import sqlalchemy
import pyodbc
import logging
import os,sys
import urllib
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.paths import PATHS
from sqlalchemy.engine import Engine
import logging

BACKORDER_REPORT_PATH = PATHS["BACKORDER_REPORT_PATH"]
SSMS_CONN_STRING = PATHS["SSMS_CONN_STRING"]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(r"H:\Upgrading_Database_Reporting_Systems\REPORTING_PIPELINE\src\logs_and_tests\backorders_upload.log",mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)

def backorder_report() -> None:
    params = urllib.parse.quote_plus(SSMS_CONN_STRING)
    tutliv_engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

    ing_backorders = pd.read_excel(BACKORDER_REPORT_PATH, header=0, dtype = {
        'EAN' : str,
        'Open Backorder' : int
    })

    #filter titles with no backorders
    ing_backorders = ing_backorders[ing_backorders['Open Backorder'] != 0]
    
    #drop rows with null EAN
    ing_backorders = ing_backorders.dropna(axis=0,subset=['EAN'])
    ing_backorders = ing_backorders.rename(columns={
        'EAN' : 'ISBN',
        'Open Backorder' : 'QTY',
        'Title' : 'TITLE'
    })

    print(ing_backorders)

    """
    Get sage backorders
    lives here : \\tutpub3\share\Sage 300 ERP\Order Entry\Backorder report_Rep & Transfer
    don't take data from there, we use the query provided in that excel sheet as it is confirmed accurate
    """
    try:
        sage_backorders = pd.read_sql(
            """
                SELECT
                    TRIM(ITEMNO) as ISBN,
                    TRIM(TITLE)  as TITLE,
                    SUM(QTYREC) as QTY
                FROM
                    TUTLIV.DBO.ALL_TRAN_BO
                GROUP BY
                    ITEMNO,
                    TITLE

            """,tutliv_engine)
    except ConnectionError as ce:
        logging.info(f'connection error {ce}')
    except Exception as e:
        logging.info(f'exception occured while {e}')
    
    #group ingram and sage backorders by ISBN,TITLE and SUM QTY
    ing_backorders = ing_backorders.groupby(['TITLE','ISBN']).agg({
        'QTY':'sum'
    }).reset_index()
    sage_backorders = sage_backorders.groupby(['TITLE','ISBN']).agg({
        'QTY' : 'sum'
    }).reset_index()

    all_backorders = pd.concat([ing_backorders,sage_backorders])

    all_backorders = all_backorders.groupby(['TITLE','ISBN']).agg({
        'QTY' : 'sum'
    }).reset_index()

    all_backorders.to_sql('BACKORDER_REPORT',con=tutliv_engine,schema='dbo',index = False,if_exists='replace')
        
if __name__ == "__main__":
    logging.info('Manual Execution Started')
    backorder_report()
    logging.info('backorder report upload complete')
