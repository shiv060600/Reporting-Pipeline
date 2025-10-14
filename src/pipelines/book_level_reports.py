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


def revenue_report(ing_sales_df: pl.DataFrame,book_details_df: pl.DataFrame,backorder_report_df: pl.DataFrame,tutliv_engine: Engine) -> None:

    """
    Columns of ing_sales_df will be
    ISBN, NETAMT, NETQTY, YEARMONTH,TITLE

    COLUMNS of book_details will be 
    TRIM([ISBN]),
    TRIM([TITLE]),TRIM([PROD_TYPE]),TRIM([PUB_DATE]),[PUB_STATUS],
    TRIM([PROD_CLASS]),TRIM([SEAS]) as SEASON,TRIM([SUB_PUB]),[RETAIL_PRICE],
    TRIM([WEBCAT1]),TRIM([WEBCAT2]),TRIM([WEBCAT2_DESCR]),TRIM([WEBCAT3]),
    TRIM([BISAC_CODE]),[QTY_ON_HAND],[QTY_ON_ORDER],TRIM([WATCH]),[CTNQTY],
    [MINRPTQTY],TRIM([GENERAL_COMMENTS]),TRIM([INTERNAL_COMMENTS]),
    TRIM([IWD]),TRIM([EXPDATE]),TRIM([SELLOFF])

    For revenue report we wil need the 
    book details columns:
    ISBN,TITLE,PROD_TYPE,SEASON,SUB_PUB,RETAIL_PRICE,QTY_ON_HAND,QTY_ON_ORDER

    backorder_report_columns:
    ISBN,TITLE,QTYBO


    """

    curr_date = datetime.datetime.now()
    curr_month = curr_date.month
    curr_year = curr_date.year

    book_details_df = book_details_df.select(['ISBN','TITLE','PROD_TYPE','SEASON','SUB_PUB','RETAIL_PRICE',
                                              'QTY_ON_HAND','QTY_ON_ORDER'])
    
    backorder_report_df = backorder_report_df.select(['ISBN','TITLE','QTYBO'])

    
    grouped_df = ing_sales_df.group_by(['ISBN','TITLE','YEARMONTH']).agg([
        pl.col('NETAMT').sum().alias('NETAMT'),
        pl.col('NETQTY').sum().alias('NETQTY')
    ])

    #base df is every possible combination of ISBN and TITLE, time series will be joined.
    #basically pivoting.
    base_df = grouped_df.select(['ISBN','TITLE']).unique()

    #ytd logic
    ytd_values = grouped_df.filter(
        (pl.col('YEARMONTH') // 100) == curr_year
    ).group_by(['ISBN','TITLE']).agg([
        pl.col('NETAMT').sum().alias('YTD_DOLLARS'),
        pl.col('NETUNITS').sum().alias('YTD_UNITS')
    ])

    #make the report DF
    report_df = base_df.join(ytd_values, on = ["ISBN","TITLE"])

    #monthly logic (last 12 months not including this month)
    year_months = []
    month_column_names = []
    for i in range(1,13):
        calc_month = curr_month - i
        calc_year = curr_year

        if calc_month <= 0:
            calc_month += 12
            calc_year -= 1
        
        datetime_obj = datetime.datetime(calc_year,calc_month,1)

        year_month = calc_year * 100 + calc_month
        year_months.append(year_month)

        column_name = datetime_obj.strftime('%b-%y')
        month_column_names.append(column_name)

        month_values = grouped_df.filter(
            pl.col('YEARMONTH') == year_month
        ).group_by(['ISBN','TITLE']).agg([
            pl.col('NETQTY').sum().alias(column_name)
        ])

        report_df = report_df.join(month_values, on = ['ISBN','TITLE'], how = 'left')

    #yearly logic(last 3 years)
    year_columns = []
    for i in range(1,4):
        calc_year = curr_year - i
        year_columns.append(calc_year)

        yearly_values = grouped_df.filter(
            (pl.col('YEARMONTH') % 100) == calc_year
        ).group_by(['ISBN','TITLE']).agg([
            pl.col('NETEUNITS').sum().alias(f"{calc_year}-UNITS")
        ])

        report_df = report_df.join(yearly_values,on = ['ISBN','TITLE'],how = 'left')

    #12 Month Rolling logic
    twelve_month_rolling_values = grouped_df.filter(
        pl.col('YEARMONTH').is_in(year_months)
    ).group_by(['ISBN','TITLE']).agg([
        pl.col('NETUNITS').sum().alias('12M_UNITS'),
        pl.col('NETAMT').sum().alias('12M_DOLLARS')
    ])

    report_df = report_df.join(twelve_month_rolling_values,on = ['ISBN','TITLE'],how = 'left')

    #join book details
    report_df = report_df.join(book_details_df,on = ['ISBN','TITLE'],how = 'left')

    #join backorder report
    report_df = report_df.join(backorder_report_df,on = ['ISBN','TITLE'], how = 'left')

    prod_df = report_df.to_pandas()

    prod_df.to_sql('REVENUE_REPORT',index=False,con= tutliv_engine,if_exists='replace',schema='dbo')

def report_a(sage_sales_df: pl.DataFrame, book_details_df: pl.DataFrame,backorder_report_df: pl.DataFrame,tutliv_engine: Engine):
    pass

def report_b(sage_sales_df: pl.DataFrame, book_details_df: pl.DataFrame,backorder_report_df: pl.DataFrame,tutliv_engine: Engine):
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
            """, tutliv_engine)
        
        unique_isbns = book_level_sales['ISBN'].unique().tolist() #unique returns numpy array t.f must use tolist() instead of to_list() like with pd.series
        join_isbns = ','.join([("'" + isbn + "'") for isbn in unique_isbns])

    except SQLAlchemyError as sqle:
        logger.error(f'Sql Alchemy error occured selecting book level sales {sqle}')
        sys.exit(1)
    except Exception as e:
        logger.error(f'general error occured selecting book level sales{e}')
        sys.exit(1)

    try:
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
    except SQLAlchemyError as sqle:
        logger.error(f'Sql Alchemy error occured selecting book details {sqle}')
        sys.exit(1)
    except Exception as e:
        logger.error(f'general error occured selecting book details {e}')
        sys.exit(1)
    
    try:
        backorder_report = pd.read_sql(
            """
            SELECT
                TRIM(ISBN) as ISBN,
                QTY as QTYBO
            FROM
                TUTLIV.dbo.BACKORDER_REPORT
            """,tutliv_engine)
    except SQLAlchemyError as sqle:
        logger.error(f'Sql Alchemy error occured selecting backorder report {sqle}')
        sys.exit(1)
    except Exception as e:
        logger.error(f'general error occured selecting backorder report {e}')
        sys.exit(1)
    
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
    revenue_report(ing_sales_df = ing_sales_df, book_details_df = book_details_df, backorder_report_df = backorder_report_df,tutliv_engine = tutliv_engine)
    logger.info('Finished revenue report')

    logger.info('Beggining report_a')
    report_a(ing_sales_df = ing_sales_df, book_details_df = book_details_df, backorder_report_df = backorder_report_df,tutliv_engine = tutliv_engine)
    logger.info('Finished report_a')

    logger.info('Begginning report_b')
    report_b(ing_sales_df = ing_sales_df, book_details_df = book_details_df, backorder_report_df = backorder_report_df,tutliv_engine = tutliv_engine)    
    logger.info('finished report_b')
    logger.info('finished all book level reports')


