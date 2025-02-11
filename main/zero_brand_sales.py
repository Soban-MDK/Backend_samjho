from utils.logger import setup_logger
from utils.db_utils import fetch_all_tables, read_local_data, store_data_to_local
from utils.transform_remote import take_requried_columns, make_data_custom_range, add_dates_to_details
from utils.transfom_input import find_replace_from_ip, join_products_bt1, get_incentive_products_bills, add_billing_id

import pandas as pd
import datetime

logger = setup_logger("zero_brand_sales")

def generate_zero_brand_report(start_date=None, end_date=None):
    # Reading and storing the remote data from the database
    try:
        sales_invoice_details = read_local_data("sales_invoice_details")
        sales_invoices = read_local_data("sales_invoices")
        sales_return = read_local_data("sales_return")
        sales_return_details = read_local_data("sales_return_details")
        products = read_local_data("products")
        stores = read_local_data("stores")
        users = read_local_data("users")
        advance_sales_invoices = read_local_data("advance_sales_invoices")  # Ensure advance_sales_invoices is read

        sales_invoice_details, sales_return_details = add_dates_to_details(sales_invoices, sales_invoice_details, sales_return, sales_return_details)
        sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = make_data_custom_range(sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices, start_date, end_date)
    
    except Exception as e:
        logger.error(f"Error fetching data - FROM ILR. {e}")
        return None
    

    try:
        sales_invoice_details = pd.merge(sales_invoice_details, products[['id', 'ws_code']], left_on='product_id', right_on='ws_code', how='left')
        sales_invoice_details.drop(columns=['id_y'], inplace=True)
        sales_invoice_details.rename(columns={'id_x': 'id'}, inplace=True)

        brands = read_local_data('brands')
        sales_invoice_details = pd.merge(sales_invoice_details, brands[['brand_cat', 'product_code']], left_on='ws_code', right_on='product_code', how='left')
        sales_invoice_details.drop(columns=['product_code'], inplace=True)

        # Merge the sales invoices and sales_invoice_details
        sales_invoice_details = pd.merge(sales_invoice_details, sales_invoices[['id', 'invoice_number', 'billing_user_id']], right_on='id', left_on='sales_invoice_id', how='left')

        # Drop the id_y column
        sales_invoice_details.drop(columns=['id_y'], inplace=True)
        sales_invoice_details.rename(columns={'id_x': 'id'}, inplace=True)

        # In the brand_cat column if the values is NaN then replcae it with 0 else convert it to 1
        sales_invoice_details['brand_cat'] = sales_invoice_details['brand_cat'].apply(lambda x: 0 if pd.isnull(x) else 1)

        sales_invoice_details['created_at'] = sales_invoice_details['created_at'].apply(lambda x: x.strftime("%Y-%m-%d"))

        # sales_invoice_details.to_csv("zero_brand_sales.csv", index=False)

    except Exception as e:
        logger.error(f"Error fetching data - FROM ILR. {e}")
        return None
    
    try:
        grouped_data = sales_invoice_details.groupby(['store_id', 'invoice_number', 'created_at', 'billing_user_id']).agg({'brand_cat': 'sum', 'bill_amount': 'sum'}).reset_index()

        # Now we need to count the total number of bills generated each day and also the total number of bills generated each day where the brand_cat is 0.
        total_bills_per_day = grouped_data.groupby(['store_id', 'created_at', 'billing_user_id']).agg({'invoice_number': 'nunique'}).reset_index()
        total_bills_per_day.rename(columns={'invoice_number': 'total_bills'}, inplace=True)

        # Count the total number of bills generated each day where the brand_cat is 0
        zero_brand_bills_per_day = grouped_data[grouped_data['brand_cat'] == 0].groupby(['store_id', 'created_at', 'billing_user_id']).agg({'invoice_number': 'nunique'}).reset_index()
        zero_brand_bills_per_day.rename(columns={'invoice_number': 'zero_brand_bills'}, inplace=True)

        # Merge the total_bills_per_day and zero_brand_bills_per_day
        grouped_data = pd.merge(total_bills_per_day, zero_brand_bills_per_day, on=['store_id', 'created_at', 'billing_user_id'], how='left')

        # Fill the NaN values with 0
        grouped_data['zero_brand_bills'] = grouped_data['zero_brand_bills'].fillna(0)

        # grouped_data.to_csv("zero_brand_sales.csv", index=False)
        store_data_to_local("Zero_brand_sales", grouped_data)
        return grouped_data

    except Exception as e:
        logger.error(f"Error fetching data - FROM ILR. {e}")
        return None
    
