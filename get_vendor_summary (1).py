import pandas as pd 
import sqlite3
import logging

# previously in ingestion_db.py script contain function ingest_db
from ingestion_db import ingest_db


#clear old handler
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)



logging.basicConfig(
    #logs folder , file get_vendor_summary.log
    filename = "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s -%(levelname)s - %(message)s",
    filemode = "a"

)


#Operations like reading data from files (e.g., pd.read_csv(), pd.read_excel()), grouping and aggregating data (e.g., df.groupby().agg()), merging or joining DataFrames (e.g., pd.merge()), and applying functions to rows or columns (e.g., df.apply()) often result in a DataFrame output.

#this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data 

def create_vendor_summary(conn): 
    vendor_sales_summary = pd.read_sql_query (""" WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber 
    ),
        PurchasesSummary AS (
            SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand, 
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price as ActualPrice,
            SUM(p.Quantity) as TotalPurchaseQuantity,
            SUM(p.Dollars) as TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        where p.PurchasePrice > 0 
        Group BY p.VendorNumber,p.VendorName,p.Brand, p.Description, p.PurchasePrice, pp.Price,pp.Volume
    ),

    SalesSummary As (
        SELECT 
            VendorNo, 
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity, 
            SUM(SalesDollars) AS TotalSalesDollars, 
            SUM(SalesPrice) AS TotalSalesPrice, 
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )


    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand, 
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity, 
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    
    FROM PurchasesSummary ps
    Left JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    Left JOIN FreightSummary fs 
        ON ps.VendorNumber = fs.VendorNumber 
    ORDER BY ps.TotalPurchaseDollars DESC""",conn) 

    return vendor_sales_summary


#df output return
#vendor_sales_summary (a DataFrame) is passed to clean_data(df) . Inside the function, that DataFrame is referenced as df
#So df refers to the DataFrame returned by create_vendor_summary(conn).




# this function will clean the data 


def clean_data(df): 

    #changing datatype to float

    df['Volume'] = df['Volume'].astype('float')


    # filling missing value with 0 

    df.fillna(0,inplace = True) 


    # removing spaces from categorical columns 
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip() 


    # creating new columns for better analysis 

    
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars'] 
    df['ProfitMargin'] =  ( df['GrossProfit'] / df['TotalSalesDollars']) *100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']


    return df 



if __name__ == '__main__':
    #creating database connection
    conn = sqlite3.connect('inventory.db') 

    logging.info('Creating Vendor Summary Table...')
    summary_df = create_vendor_summary(conn) 
    logging.info(summary_df.head())



    logging.info('Cleaning Data....') 
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())



    logging.info('Ingesting data....') 
    ingest_db(clean_df,'vendor_sales_summary',conn) 
    logging.info('Completed') 













    
    