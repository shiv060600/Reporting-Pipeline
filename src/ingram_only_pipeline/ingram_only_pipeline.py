#type: ignore
import urllib.parse
import pandas as pd
import polars as pl
import sqlalchemy
import os
import urllib,logging,sys
import pyodbc
import xlwings as xw
import datetime
import pyarrow
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.paths import PATHS


logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ingram_only_pipeline.log',mode = 'w')
    ]
)

SSMS_CONN_STRING = PATHS['SSMS_CONN_STRING']

params = urllib.parse.quote_plus(SSMS_CONN_STRING)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

#Get INGRAM sales mapping / needed data
def main():
    try:
        ingram_sales_df = pl.from_pandas(pd.read_sql(
        """
            DECLARE @start_year INT, @curr_month VARCHAR(6);

            SET @start_year = YEAR(DATEADD(YEAR,-3,GETDATE()));
            SET @curr_month = FORMAT(GETDATE(),'yyyyMM');

            SELECT
                TRIM(ING_SALES.ISBN) AS ISBN,
                TRIM(ING_SALES.TITLE) AS TITLE,
                TRIM(ING_SALES.NAMECUST) AS NAMECUST,
                ING_SALES.YEAR AS YEAR,
                ING_SALES.MONTH AS MONTH,
                TRIM(ING_SALES.[IPS Sale]) AS [IPS Sale],
                ING_SALES.NETUNITS AS NETUNITS,
                ING_SALES.NETAMT AS NETAMT,
                ING_SALES.[HQ Account Number],
                ING_SALES.[SL Account Number],
                ING_CAT.[MASTER SALES CATEGORY] as TUTTLE_SALES_CATEGORY
            FROM TUTLIV.dbo.ING_SALES AS ING_SALES
            LEFT JOIN (
                SELECT DISTINCT
                    TRIM([SL Account Number]) AS [SL Account Number],
                    TRIM([HQ Account Number]) AS [HQ Account Number],
                    TRIM([MASTER SALES CATEGORY]) AS [MASTER SALES CATEGORY]
                FROM TUTLIV.dbo.INGRAM_MASTER_CATEGORIES
            ) AS ING_CAT
                ON TRIM(ING_SALES.[SL Account Number]) = ING_CAT.[SL Account Number]
            AND TRIM(ING_SALES.[HQ Account Number]) = ING_CAT.[HQ Account Number]
            WHERE
                ING_SALES.YEAR > @start_year
                AND (ING_SALES.YEAR * 100 + ING_SALES.MONTH) != CAST(@curr_month AS INT);
        """,engine))
        logging.info(f'Successfully grabbed {len(ingram_sales_df)} records from SQL server table TUTLIV.dbo.ING_SALES')
    except Exception as error:
        logging.error(f"failed to get ingram sales {error}")

    ingram_sales_df = ingram_sales_df.with_columns(
        pl.col('ISBN').cast(pl.Utf8),
        pl.col('TITLE').cast(pl.Utf8),
        pl.col('NAMECUST').cast(pl.Utf8),
        pl.col('YEAR').cast(pl.Int32),
        pl.col('MONTH').cast(pl.Int32),
        pl.col('IPS Sale').cast(pl.Utf8),
        pl.col('NETUNITS').cast(pl.Int32),
        pl.col('NETAMT').cast(pl.Float32),
        pl.col('HQ Account Number').cast(pl.Utf8),
        pl.col('SL Account Number').cast(pl.Utf8)
    )

    base_df = ingram_sales_df.group_by(['ISBN','TITLE','NAMECUST','HQ Account Number','SL Account Number','IPS Sale','TUTTLE_SALES_CATEGORY']).agg([])
    report_df = base_df.clone()

    total_sales_before,total_units_before = ingram_sales_df.select(pl.col('NETAMT').sum(),pl.col('NETUNITS').sum()).row(0)

    logging.info(f'Total sales before processing: {total_sales_before}, Total units before processing: {total_units_before}')

    #12 Month rolling logic
    curr_dt = datetime.datetime.now()

    curr_month = curr_dt.month
    curr_year = curr_dt.year
    rolling_twelve_months_yyyymm = []

    months_dict = {
        1: 'Jan',
        2: 'Feb',
        3: 'Mar',
        4: 'Apr',
        5: 'May',
        6: 'Jun',
        7: 'Jul',
        8: 'Aug',
        9: 'Sep',
        10: 'Oct',
        11: 'Nov',
        12: 'Dec'
    }

    def yyyymm_to_units_col(yyyymm):
        year = yyyymm // 100
        month = yyyymm % 100
        return f"NET_UNITS_{months_dict[month]}_{year}"

    #monthly units logic
    
    for i in range(1,13):
        calc_month = curr_month - i
        rolling_year = curr_year
        if calc_month <= 0:
            rolling_year -= 1
            calc_month += 12
        year_month = rolling_year*100 + calc_month
        rolling_twelve_months_yyyymm.append((year_month))
        col_name = yyyymm_to_units_col(year_month)
        #do individual joins for net units per each month  12m-rolling
        calc_month_net_units = (ingram_sales_df
            .filter((pl.col('YEAR') * 100 + pl.col('MONTH')) == (year_month))
            .group_by(['ISBN','TITLE','HQ Account Number','SL Account Number','IPS Sale','TUTTLE_SALES_CATEGORY'])
            .agg(
                pl.col('NETUNITS').sum().alias(col_name)
            ))
        calc_month_net_units = calc_month_net_units.fill_null(0)
        report_df = report_df.join(
            calc_month_net_units,
            on = ['ISBN','TITLE','HQ Account Number','SL Account Number','IPS Sale','TUTTLE_SALES_CATEGORY'],
            how='left')

    #all accounts 12m rolling
    all_accounts_twelve_months_rolling = (ingram_sales_df.filter((pl.col('YEAR')* 100 + pl.col('MONTH'))
        .is_in(rolling_twelve_months_yyyymm))
        .group_by(['ISBN','TITLE'])
        .agg(
            pl.col('NETUNITS').sum().alias('ALL_ACCTS_12M_UNITS'),
            pl.col('NETAMT').sum().alias('ALL_ACCTS_12M_DOLLARS')
    ))
    all_accounts_twelve_months_rolling = all_accounts_twelve_months_rolling.fill_null(0)

    report_df = report_df.join(
        all_accounts_twelve_months_rolling,
        on = ['ISBN','TITLE'],
        how = 'left'
    )

    #individual accounts 12rolling logic
    individual_accounts_twelve_months_rolling = (ingram_sales_df.filter((pl.col('YEAR')*100 + pl.col('MONTH'))
        .is_in(rolling_twelve_months_yyyymm))
        .group_by(['ISBN','TITLE','NAMECUST','HQ Account Number','SL Account Number','IPS Sale','TUTTLE_SALES_CATEGORY'])
        .agg(
            pl.col('NETUNITS').sum().alias('12M_UNITS'),
            pl.col('NETAMT').sum().alias('12M_DOLLARS')
        ))
    individual_accounts_twelve_months_rolling = individual_accounts_twelve_months_rolling.fill_null(0)

    report_df = report_df.join(
        individual_accounts_twelve_months_rolling,
        on = ['ISBN','TITLE','NAMECUST','HQ Account Number','SL Account Number','IPS Sale','TUTTLE_SALES_CATEGORY'],
        how = 'left'
    )

    #ytd logic (curr_year line 73)
    year_to_date = (ingram_sales_df.filter(pl.col('YEAR') == curr_year)
        .group_by(['ISBN','TITLE','NAMECUST','HQ Account Number','SL Account Number','IPS Sale','TUTTLE_SALES_CATEGORY'])
        .agg(
            pl.col('NETUNITS').sum().alias('YTD_UNITS'),
            pl.col('NETAMT').sum().alias('YTD_DOLLARS')
        ))

    #prev 3 years logic
    base_year = datetime.datetime.now().year
    for year in range(base_year-1,base_year-4,-1):
        year_units = (ingram_sales_df.filter(pl.col('YEAR') == year)
            .group_by(['ISBN','TITLE','NAMECUST','HQ Account Number','SL Account Number','IPS Sale','TUTTLE_SALES_CATEGORY'])
            .agg(
                pl.col('NETUNITS').sum().alias(f'NET_UNITS_{year}')
            ))
        year_units = year_units.fill_null(0)
        report_df = report_df.join(
            year_units,
            on = ['ISBN','TITLE','NAMECUST','HQ Account Number','SL Account Number','IPS Sale','TUTTLE_SALES_CATEGORY'],
            how = 'left'
        )

    #get book metadata

    book_data = pl.from_pandas(pd.read_sql(
        """
        SELECT
            TRIM(ISBN) as ISBN,
            TRIM(PROD_TYPE) as PROD_TYPE,
            TRIM(PROD_CLASS) as PROD_CLASS,
            TRIM(SEAS) as SEAS,
            TRIM(SUBPUB) as SUBPUB,
            TRIM(WEBCAT2) as WEBCAT2,
            TRIM(WEBCAT2_DESCR) as WEBCAT2_DESCR,
            RETAIL_PRICE
        FROM TUTLIV.dbo.BOOK_DETAILS
        """,engine))

    report_df = report_df.join(
        book_data,
        on = ['ISBN'],
        how = 'left'
    )

    final_order = [
        "NAMECUST",
        "HQ Account Number",
        "SL Account Number",
        "IPS Sale",
        "TUTTLE_SALES_CATEGORY",
        "ISBN",
        "TITLE",
        "PROD_TYPE",
        "PROD_CLASS",
        "SEAS",
        "SUBPUB",
        "WEBCAT2",
        "WEBCAT2_DESCR",
        "RETAIL_PRICE",




        "12M_UNITS",
        "12M_DOLLARS",
        "YTD_UNITS",
        "YTD_DOLLARS",


        "ALL_ACCTS_12M_UNITS",
        "ALL_ACCTS_12M_DOLLARS",
    ]

    # Dynamically add all NET_UNITS_ and UNITS_ columns in order of appearance
    monthly_cols = [col for col in report_df.columns if col.startswith("NET_UNITS_")]
    yearly_cols = [col for col in report_df.columns if col.startswith("UNITS_") and col[6:].isdigit()]


    already_included = set(final_order + monthly_cols + yearly_cols)
    extra_cols = [col for col in report_df.columns if col not in already_included]

    final_order = final_order + monthly_cols + yearly_cols + extra_cols


    report_df = report_df.select([col for col in final_order if col in report_df.columns])

    report_df = report_df.fill_null(0)

    report_df = report_df.to_pandas()

    report_df.to_sql(
        name = 'COMBINED_REPORT_INGRAM_ONLY',
        schema = 'dbo',
        if_exists = 'replace',
        index = False,
        con = engine
    )

if __name__ == "__main__":
    logging.info('staring proccess')
    main()
    logging.info('finished proccess')

















