from utils.logger import setup_logger
from utils.db_utils import fetch_all_tables, read_local_data
from utils.transform_remote import take_requried_columns, make_data_custom_range, add_dates_to_details
from utils.transfom_input import find_replace_from_ip, join_products_bt1, get_incentive_products_bills, add_billing_id

import pandas as pd
import datetime

logger = setup_logger("incentive_leaderboard_report")

def generate_il_report_range(start_date=None, end_date=None):
    # Reading and storing the remote data from the database
    try:
        users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = fetch_all_tables()
        users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = take_requried_columns(users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices)
        sales_invoice_details, sales_return_details = add_dates_to_details(sales_invoices, sales_invoice_details, sales_return, sales_return_details)
        sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = make_data_custom_range(sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices, start_date, end_date)
    
    except Exception as e:
        logger.error(f"Error fetching data - FROM ILR. {e}")
        return None
    
    # Reading and storing the local data from the database in the brand_tieup_2 table
    try:
        brand_tieup_2 = read_local_data("brand_tieup_2")
        brand_tieup_2 = brand_tieup_2.dropna()
        brand_tieup_2['Month'] = brand_tieup_2['Month'].apply(find_replace_from_ip)
        brands = read_local_data("brands")
        brands = brands.dropna()

    except Exception as e:
        logger.error(f"Error fetching local data - FROM ILR. {e}")
        return None
    
    try:
        df_brand_prds = brands.merge(brand_tieup_2, on='brand_cat', how='left')
        df_brand_prds = df_brand_prds.dropna()
        df_brand_prds = df_brand_prds[["product_code", "brand_cat", "Month", "brand_sale_range", "%applied"]]

    except Exception as e:
        logger.error(f"Error joining products with brand_tieup_2 - FROM ILR. {e}")
        return None
    
    # Now we will find all the product_code which are listed in the df_brand_prds
    try:
        df_brand_prds = df_brand_prds.merge(products[['id', 'ws_code']], left_on='product_code', right_on='ws_code', how='left')
        # print("First: ", df_brand_prds.shape)
        df_brand_prds.to_csv("merged_output.csv", index=False)        
        df_brand_prds = df_brand_prds[["id", "brand_cat", "Month", "brand_sale_range", "%applied", "product_code"]]
        df_brand_prds.rename(columns={'id': 'product_id'}, inplace=True)
        # df_brand_prds.to_csv("df_brand_prds.csv", index=False)

    except Exception as e:
        logger.error(f"Error joining products with df_brand_prds - FROM ILR. {e}")
        return None
    
    #  Now we will find all the products which are listed in the df_brand_prds
    try:
        listed_prods = df_brand_prds['product_id'].unique().tolist()
        print(listed_prods)

        sales_invoice_details = sales_invoice_details[sales_invoice_details['product_id'].isin(listed_prods)]

        # sales_invoice_details.to_csv("merged_output.csv", index=False)
        # print(sales_invoice_details.head())
        
        # Add sales_invoices to get the billing_user_id
        sales_invoice_details = add_billing_id(sales_invoice_details, sales_invoices)
        # print(sales_invoice_details.head())

        # Now get the product_code from the sales_invoice_details
        sales_invoice_details = sales_invoice_details.merge(products[['id', 'ws_code']], left_on='product_id', right_on='id', how='left')
        # print("Second")

        sales_invoice_details.drop(columns=['id_y'], inplace=True)

        # Get the brand_cat from the product_code
        sales_invoice_details = sales_invoice_details.merge(brands[['brand_cat', 'product_code']], left_on='ws_code', right_on='product_code', how='left')

        print(sales_invoice_details.head())
        sales_invoice_details.to_csv("merged_output.csv", index=False)

    except Exception as e:
        logger.error(f"Error getting products from sales_invoice_details - FROM ILR. {e}")
        return None