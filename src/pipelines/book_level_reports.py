import pandas as pd
import polars as pl
import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy import inspect
import os,sys
import logging
import datetime
from sqlalchemy.exc import SQLAlchemyError, OperationalError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
"""
Book Level report, do not need to get sage sales at all for any of these books.
revenue_report makes REVENUE_REPORT table
report_a makes REPORT_A table 
report_b makes REPORT_B table

"""


logger = logging.getLogger(__name__)


def revenue_report(ing_sales_df: pl.DataFrame,book_details_df: pl.DataFrame) -> None:
    pass

def report_a(sage_sales_df: pl.DataFrame, book_details_df: pl.DataFrame):
    pass

def report_b(sage_sales_df: pl.DataFrame, book_details_df: pl.DataFrame):
    pass


def run_all_book_reports(tutliv_engine: Engine):
    """
    to get book level data we will select * FROM BOOK_LEVEL_SALES view in SQL SERVER where
    YEAR is from 10 years ago until now.
    We will then dynamically select 
    """
    book_level_sales = pd.read_sql(
        """
        @DECLARE start_year VARCHAR(4);
        @SET start_year = FORMAT(DATEADD(YEAR,-3,GETDATE()),'yyyy'))
        SELECT 
            TRIM(ISBN) as ISBN,
            TRIM(NETAMT) as NETAMT,
            NETQTY as NETQTY,
            YEAR as YEAR,
            MONTH as MONTH,
            TRIM(TITLE) as TITLE
        FROM 
            TUTLIV.dbo.BOOK_LEVEL_SALES
        WHERE 
            YEAR >= @start_year
        """
        ,tutliv_engine)
    
    unique_isbns = book_level_sales['ISBN'].unique().tolist() #unique returns numpy array t.f must use tolist() instead of to_list() like with pd.series
    join_isbns = ','.join([("'" + isbn + "'") for isbn in unique_isbns])

    #we can now select book details but only for exactly the ISBNS we need

    book_deltails = pd.read_sql(
        f"""
        SELECT
            TRIM([TITLE]),
            TRIM([PROD_TYPE]),
            TRIM([PUB_DATE]),
            [PUB_STATUS],
            TRIM([PROD_CLASS]),
            TRIM([SEAS]),
            TRIM([SUB_PUB]),
            [RETAIL_PRICE],
            TRIM([WEBCAT1]),
            TRIM([WEBCAT2]),
            TRIM([WEBCAT2_DESCR]),
            TRIM([WEBCAT3]),
            TRIM([BISAC_CODE]),
            [QTY_ON_HAND],
            [QTY_ON_ORDER],
            TRIM([WATCH]),
            [CTNQTY],
            [MINRPTQTY],
            TRIM([GENERAL_COMMENTS]),
            TRIM([INTERNAL_COMMENTS]),
            TRIM([IWD]),
            TRIM([EXPDATE]),
            TRIM([SELLOFF])
        FROM
            TUTLIV.dbo.BOOK_DETAILS
        WHERE
            ISBN IN ({join_isbns})
        """,tutliv_engine)
    
    ing_sales_df = pl.DataFrame._from_pandas(book_level_sales)
    book_details_df = pl.DataFrame._from_pandas(book_deltails)

    #now we can run each report one by one
    logger.info('Beginning Revenue Reporet')
    revenue_report(ing_sales_df = ing_sales_df, book_details_df = book_details_df)
    logger.info('Finished revenue report')

    logger.info('Beggining report_a')
    report_a(ing_sales_df = ing_sales_df, book_details_df = book_details_df)
    logger.info('Finished report_a')

    logger.info('Begginning report_b')
    report_b(ing_sales_df = ing_sales_df, book_details_df = book_details_df)
    logger.info('finished report_b')

    logger.info('finished all book level reports')


