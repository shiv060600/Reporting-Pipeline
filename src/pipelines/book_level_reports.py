import pandas as pd
import polars as pl
import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy import inspect
import os,sys
import logging
import datetime
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from polars.exceptions import ComputeError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
"""
Book Level report, do not need to get sage sales at all for any of these books.
revenue_report makes REVENUE_REPORT table
report_a makes REPORT_A table 
report_b makes REPORT_B table

"""

logger = logging.getLogger(__name__)


def revenue_report(ing_sales_df: pl.DataFrame,book_details_df: pl.DataFrame,backorder_report_df: pl.DataFrame) -> None:
    """
    Columns of ing_sales_df will be
    ISBN, NETAMT, NETQTY, YEAR, MONTH,TITLE

    COLUMNS of book_details will be 
    TRIM([ISBN]),
    TRIM([TITLE]),TRIM([PROD_TYPE]),TRIM([PUB_DATE]),[PUB_STATUS],
    TRIM([PROD_CLASS]),TRIM([SEAS]),TRIM([SUB_PUB]),[RETAIL_PRICE],
    TRIM([WEBCAT1]),TRIM([WEBCAT2]),TRIM([WEBCAT2_DESCR]),TRIM([WEBCAT3]),
    TRIM([BISAC_CODE]),[QTY_ON_HAND],[QTY_ON_ORDER],TRIM([WATCH]),[CTNQTY],
    [MINRPTQTY],TRIM([GENERAL_COMMENTS]),TRIM([INTERNAL_COMMENTS]),
    TRIM([IWD]),TRIM([EXPDATE]),TRIM([SELLOFF])

    Fore revenue report we wil need the book details 
    PROD_TYPE,SEASON,SUB_PUB,RETAIL_PRICE
    """
    curr_date = datetime.datetime.now()
    curr_month = curr_date.month
    curr_year = curr_date.year

    

    grouped_df = ing_sales_df.group_by([])


    pass

def report_a(sage_sales_df: pl.DataFrame, book_details_df: pl.DataFrame,backorder_report_df: pl.DataFrame):
    pass

def report_b(sage_sales_df: pl.DataFrame, book_details_df: pl.DataFrame,backorder_report_df: pl.DataFrame):
    pass


def run_all_book_reports(tutliv_engine: Engine):
    """
    to get book level data we will select * FROM BOOK_LEVEL_SALES view in SQL SERVER where
    YEAR is from 10 years ago until now.
    We will then dynamically select 
    """

    try:
        book_level_sales = pd.read_sql(
            """
            @DECLARE start_year VARCHAR(4);
            @SET start_year = FORMAT(DATEADD(YEAR,-3,GETDATE()),'yyyy'))
            SELECT 
                TRIM(ISBN) as ISBN,
                TRIM(TITLE) as TITLE,
                YEAR as YEAR,
                MONTH as MONTH,
                NETAMT as NETAMT,
                NETQTY as NETQTY
            FROM 
                TUTLIV.dbo.BOOK_LEVEL_SALES
            WHERE 
                YEAR >= @start_year
            """
            ,tutliv_engine)
        
        unique_isbns = book_level_sales['ISBN'].unique().tolist() #unique returns numpy array t.f must use tolist() instead of to_list() like with pd.series
        join_isbns = ','.join([("'" + isbn + "'") for isbn in unique_isbns])

    except SQLAlchemyError as sqle:
        logger.error(f'Sql Alchemy error occured selecting book level sales {sqle}')
        sys.exit(1)
    except Exception as e:
        logger.error(f'general error occured selecting book level sales{e}')
        sys.exit(1)

    #we can now select book details but only for exactly the ISBNS we need

    book_deltails = pd.read_sql(
        f"""
        SELECT
            TRIM([ISBN]),
            TRIM([TITLE]),
            TRIM([PROD_TYPE]),
            TRIM([PUB_DATE]),
            [PUB_STATUS],
            TRIM([PROD_CLASS]),
            TRIM([SEAS]) as SEASON,
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
    
    backorder_report = pd.read_sql(
        """
        SELECT
            TRIM(ISBN) as ISBN,
            QTY as QTYBO
        FROM
            TUTLIV.dbo.BACKORDER_REPORT
        """,tutliv_engine)
    
    ing_sales_df = pl.DataFrame._from_pandas(book_level_sales)
    book_details_df = pl.DataFrame._from_pandas(book_deltails)
    backorder_report_df = pl.DataFrame._from_pandas(backorder_report)

    #cast types once before passing

    ing_sales_df = ing_sales_df.with_columns(
        pl.col('ISBN').cast(pl.Utf8),
        pl.col('TITLE').cast(pl.Utf8),
        pl.col('YEAR').cast(pl.Int32),
        pl.col('MONTH').cast(pl.Int32),
        pl.col('NETAMT').cast(pl.Int64),
        pl.col('NETQTY').cast(pl.Float64)
    ).with_columns(
        (pl.col('YEAR') * 100 + pl.col('MONTH').cast(pl.Int64).alias('YEARMONTH'))
    ).drop(['YEAR','MONTH'])

    book_details_df = book_details_df.with_columns(
        pl.col('ISBN').cast(pl.Utf8),
        pl.col('TITLE').cast(pl.Utf8),
        pl.col('PROD_TYPE').cast(pl.Utf8),
        pl.col('PUB_DATE').cast(pl.Utf8),
        pl.col('PUB_STATUS').cast(pl.Int32),
        pl.col('PROD_CLASS').cast(pl.Utf8),
        pl.col('SEASON').cast(pl.Utf8),
        pl.col('SUB_PUB').cast(pl.Utf8),
        pl.col('RETAIL_PRICE').cast(pl.Float64),
        pl.col('WEBCAT1').cast(pl.Utf8),
        pl.col('WEBCAT2').cast(pl.Utf8),
        pl.col('WEBCAT2_DESCR').cast(pl.Utf8),
        pl.col('WEBCAT3').cast(pl.Utf8),
        pl.col('BISAC_CODE').cast(pl.Utf8),
        pl.col('QTY_ON_HAND').cast(pl.Int32),
        pl.col('QTY_ON_ORD').cast(pl.Float64),
        pl.col('WATCH').cast(pl.Utf8),
        pl.col('CTNQTY').cast(pl.Int32),
        pl.col('MINRPTQTY').cast(pl.Int32),
        pl.col('GENERAL_COMMENTS').cast(pl.Utf8),
        pl.col('INTERNAL_COMMENTS').cast(pl.Utf8),
        pl.col('IWD').cast(pl.Utf8),
        pl.col('EXPDATE').cast(pl.Utf8),
        pl.col('SELLOFF').cast(pl.Utf8)
    )

    backorder_report_df = backorder_report_df.with_columns(
        pl.col('ISBN').cast(pl.Utf8),
        pl.col('QTYBO').cast(pl.Int32)
    )

    #now we can run each report one by one
    logger.info('Beginning Revenue Reporet')
    revenue_report(ing_sales_df = ing_sales_df, book_details_df = book_details_df, backorder_report_df = backorder_report_df)
    logger.info('Finished revenue report')

    logger.info('Beggining report_a')
    report_a(ing_sales_df = ing_sales_df, book_details_df = book_details_df, backorder_report_df = backorder_report_df)
    logger.info('Finished report_a')

    logger.info('Begginning report_b')
    report_b(ing_sales_df = ing_sales_df, book_details_df = book_details_df, backorder_report_df = backorder_report_df)
    logger.info('finished report_b')
    logger.info('finished all book level reports')


