import os
import pandas as pd
from utils.logger import setup_logger
from utils.db_utils import get_local_engine

logger = setup_logger("transform_input")

def find_replace_from_ip(months_year):
    find_replace = {
        "Jan": "January",
        "Feb": "February",
        "Mar": "March",
        "Apr": "April",
        "May": "May",
        "Jun": "June",
        "Jul": "July",
        "Aug": "August",
        "Sep": "September",
        "Oct": "October",
        "Nov": "November",
        "Dec": "December",
    }

    for key, value in find_replace.items():
        if key in months_year:
            return months_year.replace(key, value)
        
    return months_year

def join_products_bt1(products, brand_tieup_1):
    try:
        brand_tieup_1 = brand_tieup_1.dropna()
        brand_tieup_1 = pd.merge(brand_tieup_1, products[['ws_code', 'product_name', 'id']], left_on='product code', right_on='ws_code', how='left').drop('ws_code', axis=1)
        brand_tieup_1.rename(columns={'id': 'product_id', 'product code':'ws_code'}, inplace=True)
        # print(brand_tieup_1.head())

    except Exception as e:
        logger.error(f"Error joining products with brand_tieup_1: {e}")
    
    return brand_tieup_1

def get_incentive_products_bills(sales_invoice_details, brand_tieup_1):
    # I need to filter out all the product_ids from sales_invoice_details that are present in brand_tieup_1
    try:
        product_ids = brand_tieup_1['product_id'].unique()
        incentive_products_bills = sales_invoice_details[sales_invoice_details['product_id'].isin(product_ids)]        
        logger.info(f"Found {len(incentive_products_bills)} incentive products bills.")
        
        return incentive_products_bills

    except Exception as e:
        logger.error(f"Error getting incentive products bills: {e}")
        return None
    
def add_billing_id(sales_invoice_details, sales_invoices):
    try:
        sales_invoice_details = pd.merge(sales_invoice_details, sales_invoices[['id', 'billing_user_id']], left_on='sales_invoice_id', right_on='id', how='left')
        if 'id_y' in sales_invoice_details.columns:
            sales_invoice_details.drop('id_y', axis=1, inplace=True)
            sales_invoice_details.rename(columns={'id_x': 'id'}, inplace=True)
        logger.info("Added billing user id to sales invoice details.")
    
    except Exception as e:
        logger.error(f"Error adding billing user id to sales invoice details: {e}")
    
    return sales_invoice_details