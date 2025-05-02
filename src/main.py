import pandas as pd
import numpy as np
from dbfread import DBF
import sqlalchemy
import xlwings as xw
import urllib
from helpers.paths import PATHS
import datetime
import json
from rapidfuzz import process, fuzz
import sys
import time

SSMS_CONN_STRING = PATHS["SSMS_CONN_STRING"]
INGRAM_SALES_DBF = PATHS["INGRAM_SALES_DBF"]
INGRAM_CUSTCODES = PATHS["ACCT_CODES_DESCR_INGRAM"]
ISBN_WEBCAT = PATHS["ISBN_WEBCAT"]
SAGE_CUST_CODES = PATHS["SAGE_CUST_CODES"]
JSON_CUST_CODES = PATHS["JSON_CUST_CODES"]
EXPORT_XL = PATHS["ALL_SALES_INCL_ING"] 

params = urllib.parse.quote_plus(SSMS_CONN_STRING)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}",connect_args={'timeout':1800,'connect_timeout':120},pool_recycle=3600)

def main():
    # read ingram dbf
    ingram_sales_dbf = DBF(INGRAM_SALES_DBF)
    ingram_sales_df = pd.DataFrame(iter(ingram_sales_dbf))

    # clean ingram data as wanted
    ingram_sales_df = ingram_sales_df[~ingram_sales_df["ISBN"].str.strip().isin(["EAN", "1", "2"])]
    ingram_sales_df = ingram_sales_df[ingram_sales_df["ISBN"].str.strip() != ""]
    ingram_sales_df = ingram_sales_df.dropna(subset=['ISBN', 'COMPANY', 'INV_DT']) 

    # format date columns
    ingram_sales_df["INV_DT"] = pd.to_datetime(ingram_sales_df["INV_DT"],format="%m/%d/%Y")
    ingram_sales_df["MONTH"] = ingram_sales_df["INV_DT"].dt.month
    ingram_sales_df["YEAR"] = ingram_sales_df["INV_DT"].dt.year
    ingram_sales_df = ingram_sales_df.drop(columns=["INV_DT","KEY"], errors='ignore')

    # filter last 3 years
    start_year = (datetime.datetime.now()).year - 3
    ingram_sales_df = ingram_sales_df[ingram_sales_df["YEAR"] > start_year]

    # read sage sales (excluding ingram)
    sage_sales_no_ingram_df = pd.read_sql(
    """
    DECLARE @StartYear Varchar(6);
    DECLARE @CurrrentMonth Varchar(6);

    SET @StartYear = YEAR(DATEADD(YEAR,-3,GETDATE()));
    SET @CurrrentMonth = FORMAT(GETDATE(),'yyyMM');

    SELECT
        TRIM(customers.NEWCUST) as BILLTO,
        TRIM(customers.NAMECUST) as NAMECUST,
        TRIM(sales.ISBN) as ISBN,
        TRIM(sales.TITLE) as TITLE,
        sales.YEAR,
        sales.MONTH,
        sales.NETAMT,
        sales.NETQTY
    FROM ALL_SALES_HSA as sales
    LEFT JOIN ALL_CUSTMAST as customers on sales.NEWBILLTO = customers.NEWCUST
    WHERE 
        sales.YEAR >= @StartYear
        AND (sales.YEAR*100 + sales.MONTH) <> @CurrrentMonth
        AND customers.NAMECUST NOT LIKE '%INGRAM BOOK CO.%'
    """,engine)

    sage_names_list = pd.Series(sage_sales_no_ingram_df['NAMECUST'].dropna().unique())
    ingram_unique_names = ingram_sales_df["COMPANY"].unique()

    # map ingram names to sage names (fuzzy match (fast))
    name_mapping = {}
    if not sage_names_list.empty:
        for ingram_name in ingram_unique_names:
            if pd.isna(ingram_name): continue
            best_match,score,_= process.extractOne(str(ingram_name),sage_names_list ,scorer=fuzz.token_sort_ratio)
            if score >= 70: 
                name_mapping[ingram_name] = best_match
            else:
                name_mapping[ingram_name] = ingram_name 
    else:
        for ingram_name in ingram_unique_names:
            if not pd.isna(ingram_name):
                name_mapping[ingram_name] = ingram_name

    # apply name mapping
    ingram_sales_df['SAGE_COMPANY_NAME'] = ingram_sales_df['COMPANY'].map(name_mapping)

    # read ingram custcodes excel
    try:
        ing_custcodes_app = xw.App(visible=False)
        ing_custcodes_app_wb = ing_custcodes_app.books.open(INGRAM_CUSTCODES)
        acct_codes_ws = ing_custcodes_app_wb.sheets["acctcodes"]
        ing_custcodes_df = acct_codes_ws.range("A1").options(pd.DataFrame,header = 1,index = False, expand='table').value 
    finally:
        if 'ing_custcodes_app_wb' in locals() and hasattr(ing_custcodes_app_wb, 'name') and ing_custcodes_app_wb.name in [b.name for b in ing_custcodes_app.books]: ing_custcodes_app_wb.close()
        if 'ing_custcodes_app' in locals() and hasattr(ing_custcodes_app, 'pid') and ing_custcodes_app.pid: ing_custcodes_app.quit()

    # merge ingram sales with custcodes
    ingram_sales_df = pd.merge(ingram_sales_df,ing_custcodes_df,how='left', left_on = "GROUPCODE" , right_on = "WHOLESALE")

    # read webcat codes excel
    try:
        ing_webcat_app = xw.App(visible = False)
        ing_webcat_wb = ing_webcat_app.books.open(ISBN_WEBCAT)
        webcat_codes_ws = ing_webcat_wb.sheets['Sheet1']
        webcat_codes_df = webcat_codes_ws.range("A1").options(pd.DataFrame,index = False,header = 1,expand = 'table').value
    finally:
        if 'ing_webcat_wb' in locals() and hasattr(ing_webcat_wb, 'name') and ing_webcat_wb.name in [b.name for b in ing_webcat_app.books]: ing_webcat_wb.close()
        if 'ing_webcat_app' in locals() and hasattr(ing_webcat_app, 'pid') and ing_webcat_app.pid : ing_webcat_app.quit()

    # merge ingram sales with webcat
    ingram_sales_df = pd.merge(ingram_sales_df,webcat_codes_df,how='left',left_on='ISBN',right_on='ISBN_13DIGIT')

    # merge sage sales with webcat
    sage_sales_no_ingram_df = pd.merge(sage_sales_no_ingram_df,webcat_codes_df,how="left",left_on="ISBN",right_on="ISBN_13DIGIT")
    
    # merge sage sales with sage cust codes dbf
    try: 
        sage_cust_codes_dbf = DBF(SAGE_CUST_CODES)
        sage_cust_codes_df = pd.DataFrame(iter(sage_cust_codes_dbf))
        sage_cust_codes_df = sage_cust_codes_df[['ACCTNUM', 'GROUPCODE']] 
        sage_sales_no_ingram_df = pd.merge(sage_sales_no_ingram_df,sage_cust_codes_df,how="left",left_on="BILLTO",right_on="ACCTNUM")
    except Exception as e:
        print(f"Error loading or merging Sage DBF customer codes: {e}")

    # merge sage sales with json cust code descriptions
    try: 
        with open(JSON_CUST_CODES,"r") as file:
            data = json.load(file)
            json_cust_codes_df = pd.DataFrame(list(data.items()),columns=["ACCTCODE","ACCTDESCR"])
        sage_sales_no_ingram_df = pd.merge(sage_sales_no_ingram_df,json_cust_codes_df,how='left',left_on="GROUPCODE",right_on = "ACCTCODE")
    except Exception as e:
        print(f"Error loading or merging JSON customer codes: {e}")

    # drop unnecessary columns
    ingram_sales_df = ingram_sales_df.drop(columns=["COMPANY", "ISBN_13DIGIT", "WHOLESALE", "GROUPCODE"], errors='ignore') 
    sage_sales_no_ingram_df = sage_sales_no_ingram_df.drop(columns=["ACCTMNGR", "MKTSEG", "ACCTNUM", "ISBN_13DIGIT", "GROUPCODE", "LONG_TITLE", "ACCTCODE_x", "ACCTCODE_y"], errors='ignore') 

    # rename columns for consistency
    ingram_sales_df = ingram_sales_df.rename(columns = {
        "QTY" : "NETQTY" , 
        "EXTPRICE" : "NETAMT" , 
        "SAGE_COMPANY_NAME" : "NAMECUST", 
        "LONG_TITLE" : "TITLE",          
        "CODE" : "ACCTCODE" ,            
        "CODEDESCR":"ACCTDESCR"          
    })

    # define final column structure
    final_columns_order = [
        'BILLTO', 
        'NAMECUST', 
        'ISBN', 
        'TITLE', 
        'YEAR', 
        'MONTH', 
        'NETAMT', 
        'NETQTY', 
        'ACCTCODE', 
        'ACCTDESCR', 
        'WEBCAT1 Code', 
        'webcat1 Description', 
        'WEBCAT2 CODE', 
        'webcat2 Description'
    ]

    # align columns before combining
    ingram_sales_df = ingram_sales_df.reindex(columns=final_columns_order)
    sage_sales_no_ingram_df = sage_sales_no_ingram_df.reindex(columns=final_columns_order)

    # combine dataframes
    combined_sales_df = pd.concat([ingram_sales_df, sage_sales_no_ingram_df], ignore_index=True)

    # ensure numeric types and fill NaNs
    combined_sales_df['NETAMT'] = pd.to_numeric(combined_sales_df['NETAMT'], errors='coerce')
    combined_sales_df['NETQTY'] = pd.to_numeric(combined_sales_df['NETQTY'], errors='coerce')
    combined_sales_df = combined_sales_df.fillna({'NETAMT': 0, 'NETQTY': 0}) 

    # define grouping keys
    grouping_keys = ['NAMECUST', 'TITLE', 'ISBN', 'MONTH', 'YEAR']

    # define aggregations
    aggregations = {
        'NETAMT': 'sum',
        'NETQTY': 'sum',
        'BILLTO': 'first', 
        'ACCTCODE': 'first', 
        'ACCTDESCR': 'first', 
        'WEBCAT1 Code': 'first',
        'webcat1 Description': 'first',
        'WEBCAT2 CODE': 'first',
        'webcat2 Description': 'first'
    }

    # group and aggregate data
    final_grouped_df = combined_sales_df.groupby(grouping_keys, as_index=False).agg(aggregations)

    # define final export column order
    final_export_order = [
        'BILLTO',               
        'NAMECUST',             
        'YEAR',                 
        'MONTH',                
        'ISBN',                 
        'TITLE',                
        'NETAMT',           
        'NETQTY',            
        'ACCTCODE',             
        'ACCTDESCR',    
        'WEBCAT2 CODE',            
        'webcat2 Description'   
    ]

    final_grouped_df = final_grouped_df[final_export_order]
    
    #Preform calculations to get monthly, yearly, and ytd figures
    curr_date = datetime.datetime.now()
    curr_year = curr_date.year
    curr_month = curr_date.month

    final_grouped_df['YEARMONTH']= final_grouped_df['YEAR'] * 100 + final_grouped_df['MONTH']

    base_df = final_grouped_df.groupby(['ISBN','TITLE','NAMECUST']).first().reset_index()
    base_df = base_df[['ISBN', 'TITLE', 'NAMECUST', 'ACCTCODE', 'ACCTDESCR', 'WEBCAT2 CODE', 'webcat2 Description']]

    #YTD Values
    ytd_dollars = final_grouped_df[final_grouped_df["YEAR"] == curr_year].groupby(['ISBN','TITLE','NAMECUST']).agg({'NETAMT':'sum'}).reset_index()
    ytd_dollars = ytd_dollars.rename(columns = {'NETAMT':'YTD_DOLLARS'})

    ytd_units = final_grouped_df[final_grouped_df["YEAR"] == curr_year].groupby(['ISBN','TITLE','NAMECUST']).agg({"NETQTY":"sum"}).reset_index()
    ytd_units = ytd_units.rename(columns={"NETQTY":"YTD_UNITS"})

    report_df = pd.merge(base_df,ytd_dollars,how = 'left',on = ['ISBN','TITLE','NAMECUST'])
    report_df['YTD_DOLLARS'] = report_df['YTD_DOLLARS'].fillna(0)

    report_df = pd.merge(report_df,ytd_units,how='left',on=['ISBN','TITLE','NAMECUST'])
    report_df["YTD_UNITS"] = report_df['YTD_UNITS'].fillna(0)

    #Totals
    total_dollars = final_grouped_df.groupby(['ISBN','TITLE','NAMECUST']).agg({'NETAMT':'sum'}).reset_index()
    total_dollars = total_dollars.rename(columns={"NETAMT":"TOTAL_DOLLARS"})

    total_units = final_grouped_df.groupby(["ISBN","TITLE","NAMECUST"]).agg({"NETQTY":"sum"}).reset_index()
    total_units = total_units.rename(columns = {"NETQTY":'TOTAL_UNITS'})

    report_df = pd.merge(report_df,total_dollars,how = 'left',on = ['ISBN','TITLE','NAMECUST'])
    report_df["TOTAL_DOLLARS"] = report_df['TOTAL_DOLLARS'].fillna(0)

    report_df = pd.merge(report_df,total_units,how = 'left',on = ['ISBN','TITLE','NAMECUST'])
    report_df["TOTAL_UNITS"] = report_df['TOTAL_UNITS'].fillna(0)

    #Monthly Sums Logic
    for i in range(1,13):
        calc_month = curr_month - i
        calc_year = curr_year

        if calc_month <= 0:
            calc_month += 12
            calc_year -= 1
        
        #Create column name
        month_name = datetime.datetime(calc_year, calc_month, 1).strftime('%b-%y')
        yearmonth = calc_year * 100 + calc_month

        month_units_df = final_grouped_df[final_grouped_df['YEARMONTH'] == yearmonth].groupby(['ISBN','TITLE','NAMECUST']).agg({'NETQTY':'sum'}).reset_index()
        month_units_df = month_units_df.rename(columns = {"NETQTY":f"UNITS_{month_name}"})

        report_df = pd.merge(report_df, month_units_df, on=['ISBN', 'TITLE', 'NAMECUST'], how='left')
        report_df[f'UNITS_{month_name}'] = report_df[f'UNITS_{month_name}'].fillna(0)
    
    #Yearly Sums Logic
    for year in range(curr_year - 3, curr_year + 1):
        year_units_df = final_grouped_df[final_grouped_df['YEAR'] == year].groupby(['ISBN','TITLE','NAMECUST']).agg({'NETQTY':'sum'}).reset_index()
        year_units_df = year_units_df.rename(columns = {"NETQTY":f"UNITS_{year}"})
        
        report_df = pd.merge(report_df, year_units_df, on=['ISBN', 'TITLE', 'NAMECUST'], how='left')
        report_df[f'UNITS_{year}'] = report_df[f'UNITS_{year}'].fillna(0)
    
    #Reorder columns 
    cols = report_df.columns.tolist()
    key_cols = ['ISBN', 'TITLE', 'NAMECUST', 'ACCTDESCR', 'webcat2 Description', 
                'YTD_DOLLARS', 'YTD_UNITS', 'TOTAL_DOLLARS', 'TOTAL_UNITS']
 
    month_cols = [col for col in cols if col.startswith('UNITS_') and not col[-4:].isdigit()]
    year_cols = [col for col in cols if col.startswith('UNITS_') and col[-4:].isdigit()]
    
    final_order = key_cols + month_cols + year_cols
    report_df = report_df[final_order]

    # export final data to excel
    try:
        app = xw.App(visible=False)
        with app.books.open(EXPORT_XL) as wb:
            ws = wb.sheets['DATA'] 
            ws.clear_contents() 
            ws.range("A1").options(index = False, header=True).value = report_df
            ws.range("A:E").column_width = 25
            ws.range("F:Z").column_width = 12
            wb.save()
            print(f"Successfully exported grouped data to {EXPORT_XL}")
    except Exception as e:
        print(f"Error writing to Excel: {e}")
    finally:
        if 'app' in locals() and hasattr(app, 'pid') and app.pid: app.quit()

if __name__ == "__main__":
    main()
    print("finished proccessing")
    time.sleep(3)
    sys.exit(0)
