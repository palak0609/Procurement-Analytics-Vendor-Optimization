# This is created after EDA 
# This is a Script to get the vendor summary from the database
# if we want to schedule the repititive task, we can use this script
import pandas as pd
import sqlite3
import logging
import os
from ingestion_db import ingest_db, engine  # Import the engine from ingestion_db
import time

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Create a specific logger for this script
logger = logging.getLogger('vendor_summary')
logger.setLevel(logging.DEBUG)

# Create file handler
file_handler = logging.FileHandler('logs/get_vendor_summary.log', mode='a')
file_handler.setLevel(logging.DEBUG)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(file_handler)



def create_vendor_summary(con):
    '''this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""WITH 
    freight_summary AS (
        SELECT 
            VendorNumber, 
            SUM(Freight) as FreightCost 
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    purchase_summary AS (
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
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Volume, pp.Price
    ),

    sales_summary AS (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesQuantity) as TotalSalesQuantity,
            SUM(SalesDollars) as TotalSalesDollars,
            SUM(SalesPrice) as TotalSalesPrice,
            SUM(ExciseTax) as TotalExciseTax
        FROM sales 
        GROUP BY VendorNo, Brand
    )

    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.Volume,
        ps.ActualPrice,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM purchase_summary ps
    LEFT JOIN sales_summary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN freight_summary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""", con)

    return vendor_sales_summary



def clean_data(df):
    '''this function will clean the data'''
    # changing datatype to float
    df['Volume'] = df['Volume'].astype('float64')

    # filling missing values with 0
    df.fillna(0, inplace=True)

    # removing the white spaces from the categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # creating new columns
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df 



if __name__ == "__main__":
    start = time.time()
    # creating the connection to the database
    con = sqlite3.connect('inventory.db')

    logger.info("Creating vendor summary")
    summary_df = create_vendor_summary(con)
    logger.info(summary_df.head())

    logger.info("Cleaning data")
    clean_df = clean_data(summary_df)
    logger.info(clean_df.head())

    logger.info("Ingesting data into database")
    ingest_db(clean_df, "vendor_sales_summary", con)

    logger.info("Data ingested successfully")

    end = time.time()
    logger.info(f"Time taken: {end - start} seconds")


