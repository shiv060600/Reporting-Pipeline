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


logger = logging.getLogger(__name__)


def revenue_report(ing_sales_df: pl.DataFrame,sage_sales_df: pl.DataFrame, tutliv_engine: Engine) -> None:
    """
    ing_sales_df columns: 
    HQ_NUMBER, SL_NUMBER, ISBN, YEAR, MONTH, TITLE, NAMECUST, NETUNITS, NETAMT, TUTTLE_SALES_CATEOGRY

    sage_sales_df columns:
    SAGE_ID, ISBN, YEAR, MONTH, TITLE, NAMECUST, NETUNITS, NETAMT, TUTTLE_SALES_CATEGORY

    This report is a book level report, we dont need any customer ids,names, or sales categories
    We will do grouping to get required monthly, yearly columns and then join on book details.
    """
    ing_sales_df = ing_sales_df.select(['ISBN','TITLE','YEAR','MONTH','NETUNITS','NETAMT'])
    sage_sales_df = sage_sales_df.select(['ISBN','TITLE','YEAR','MONTH','NETUNITS','NETAMT'])

    ing_sales_df = ing_sales_df.with_columns([
    pl.col('ISBN').cast(pl.Utf8),
    pl.col('TITLE').cast(pl.Utf8),
    pl.col('YEAR').cast(pl.Int64),
    pl.col('MONTH').cast(pl.Int64),
    pl.col('NETUNITS').cast(pl.Int64),
    pl.col('NETAMT').cast(pl.Float64),
    ])

    sage_sales_df = sage_sales_df.with_columns([
        pl.col('ISBN').cast(pl.Utf8),
        pl.col('TITLE').cast(pl.Utf8),
        pl.col('YEAR').cast(pl.Int64),
        pl.col('MONTH').cast(pl.Int64),
        pl.col('NETUNITS').cast(pl.Int64),
        pl.col('NETAMT').cast(pl.Float64),
    ])
    #concat vertically for grouping
    combined_df = pl.concat([ing_sales_df,sage_sales_df],how = 'vertical')

    #define dates
    curr_date = datetime.datetime.now()
    curr_year = curr_date.year
    curr_month = curr_date.month

    #cast dtypes and create yearmonthcolumn
    #drop year and month after
    combined_df = combined_df.with_columns(
        pl.col('ISBN').cast(pl.Utf8).alias('ISBN'),
        pl.col('TITLE').cast(pl.Utf8).alias('TITLE'),
        pl.col('NETUNITS').cast(pl.Int64).alias('NETUNITS'),
        pl.col('NETAMT').cast(pl.Float64).alias('NETAMT'),
    ).with_columns(
        (pl.col('YEAR') * 100 + pl.col('MONTH')).cast(pl.Int64).alias('YEARMONTH')
    ).drop(['YEAR','MONTH'])

    #perform grouping on combined df
    combined_df = combined_df.group_by(['ISBN','TITLE','YEARMONTH']).agg([
        pl.col('NETUNITS').sum().alias('NETUNITS'),
        pl.col('NETAMT').sum().alias('NETAMT'),
    ])

    base_df = combined_df.select(['ISBN','TITLE']).unique()


    #ytd_logic
    ytd_values = combined_df.filter(
        (pl.col('YEARMONTH') // 100) == curr_year
    ).group_by(['ISBN','TITLE']).agg([
        pl.col('NETUNITS').sum().alias('YTD_UNITS'),
        pl.col('NETAMT').sum().alias('YTD_DOLLARS')
    ])

    #Create initial report df by joining base with YTD values
    report_df = base_df.join(ytd_values, on = ['ISBN','TITLE'], how = 'left')

    #12month unit cols
    twelve_month_names = []
    yearmonth_columns = []
    for i in range(1,13):
        calc_month = curr_month - i
        calc_year = curr_year

        if calc_month <= 0:
            calc_month += 12
            calc_year -= 1
        datetime_object = datetime.datetime(year = calc_year,month  =calc_month, day = 1)
        year_month_filter = calc_year * 100 + calc_month

        col_name = datetime.datetime.strftime(datetime_object,format = '%b-%y')
        twelve_month_names.append(col_name)
        yearmonth_columns.append(year_month_filter)

        #only need units for monthy columns
        monthly_units = combined_df.filter(
            pl.col('YEARMONTH') == year_month_filter
        ).group_by(['ISBN','TITLE']).agg([
            pl.col('NETUNITS').sum().alias(col_name)
        ]).fill_null(0)


        #join to reportdf
        report_df = report_df.join(monthly_units, on = ['ISBN','TITLE'], how = 'left')
    
    #yearly unit cols
    yearly_col_names = []
    for i in range(1,4):
        calc_year = curr_year - i

        yearly_units = combined_df.filter(
            (pl.col('YEARMONTH') // 100) == calc_year
        ).group_by(['ISBN','TITLE']).agg([
            pl.col('NETUNITS').sum().fill_null(0).alias(f"{calc_year}-UNITS")
        ])
        yearly_col_names.append(f"{calc_year}-UNITS")

        #join to report df
        report_df = report_df.join(yearly_units, on = ['ISBN','TITLE'], how = 'left')
    
    #12_month_rolling logc
    twelve_month_rolling_values = combined_df.filter(
        pl.col('YEARMONTH').is_in(yearmonth_columns)
    ).group_by(['ISBN','TITLE']).agg([
        pl.col('NETUNITS').sum().fill_null(0).alias('12M_UNITS'),
        pl.col('NETAMT').sum().fill_null(0).alias('12M_DOLLARS')
    ])

    report_df: pl.DataFrame = report_df.join(twelve_month_rolling_values,on = ['ISBN','TITLE'], how = 'left')

    #get unique isbns
    used_isbns = report_df.select(['ISBN']).unique().to_series().to_list()
    if not used_isbns:
        logger.error("No ISBNs found to query book details.")
        sys.exit(1)

    isbn_list_sql = ','.join([f"'{isbn}'" for isbn in used_isbns])
    query = f"""
        SELECT 
            TRIM(BD.ISBN) as ISBN,
            TRIM(BD.PROD_TYPE) as PROD_TYPE,
            TRIM(BD.SEASON_CODE) as SEASON_CODE,
            TRIM(BD.SUB_PUB) as SUB_PUB_CODE,
            TRIM(BD.WEBCAT2_DESCR) as CATEGORY,
            BD.RETAIL_PRICE,
            BD.QTYOH,
            BO.QTY as QTYBO,
            BD.QTYORD
        FROM 
            (
                SELECT
                    ISBN,
                    PROD_TYPE,
                    SEAS as SEASON_CODE,
                    SUB_PUB,
                    RETAIL_PRICE,
                    WEBCAT2_DESCR,
                    QTY_ON_HAND as QTYOH,
                    QTY_ON_ORDER as QTYORD
                FROM
                    TUTLIV.dbo.BOOK_DETAILS
                WHERE
                    TRIM(ISBN) IN ({isbn_list_sql})
            ) as BD 
        LEFT JOIN
            TUTLIV.dbo.BACKORDER_REPORT as BO
            on TRIM(BO.ISBN) = TRIM(BD.ISBN)
    """
    try:
        book_details_df = pd.read_sql(query,tutliv_engine)
        
    except OperationalError as oe:
        logger.error(f'Database Connection error: {oe}')
    except SQLAlchemyError as sqle:
        logger.error(f"sqlachemy error: {sqle}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    
    final_df = report_df.to_pandas()

    if book_details_df.shape[0] > 0:
        final_df = final_df.merge(book_details_df,on=['ISBN'],how='left')
        final_df = final_df.fillna(0,axis=0)
    else:
        logger.error('report failed to fill book details, exiting now')
        sys.exit(1)

    final_order = ['TITLE','ISBN','CATEGORY','PROD_TYPE',
                   'SEASON_CODE','SUB_PUB_CODE','RETAIL_PRICE',
                   'QTYOH','QTYBO','QTYORD','12M_UNITS','12M_DOLLARS',
                   'YTD_UNITS','YTD_DOLLARS'] + twelve_month_names + yearly_col_names

    final_df = final_df[final_order]
    final_df = final_df.sort_values(by = ['CATEGORY','12M_DOLLARS'], ascending=[False,False])

    final_df.to_sql(
            'REVENUE_REPORT',
            tutliv_engine,
            schema="dbo",
            if_exists="replace",
            index=False
        )


def report_a(ing_sales_df: pl.DataFrame,sage_sales_df: pl.DataFrame, tutliv_engine: Engine):
    pass

def report_b(ing_sales_df: pl.DataFrame,sage_sales_df: pl.DataFrame, tutliv_engine: Engine):
    pass

def run_all_book_reports(ing_sales_df: pl.DataFrame,sage_sales_df: pl.DataFrame, tutliv_engine: Engine):
    pass

