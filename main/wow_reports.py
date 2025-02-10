from utils.logger import setup_logger
from utils.db_utils import fetch_all_tables, read_local_data
from utils.transform_remote import take_requried_columns, make_data_custom_range, add_dates_to_details, prod_category
from utils.transfom_input import find_replace_from_ip, join_products_bt1, get_incentive_products_bills, add_billing_id

import pandas as pd
import datetime

logger = setup_logger("stores_month_targets")

def generate_wow_reports(months_year=None):

    try:
        sales_invoice_details = read_local_data("sales_invoice_details")
        sales_invoices = read_local_data("sales_invoices")
        sales_return = read_local_data("sales_return")
        sales_return_details = read_local_data("sales_return_details")
        products = read_local_data("products")
        stores = read_local_data("stores")
        users = read_local_data("users")
        
        sales_invoice_details, sales_return_details = add_dates_to_details(sales_invoices, sales_invoice_details, sales_return, sales_return_details)
        sales_invoice_details = pd.merge(sales_invoice_details, sales_invoices[['id', 'invoice_number']], left_on='sales_invoice_id', right_on='id', how='left')
        sales_invoice_details.drop(columns=['id_y'], inplace=True)
        sales_invoice_details.rename(columns={'sales_invoice_id': 'sales_id', 'id_x': 'id'}, inplace=True)

        sales_return_details = pd.merge(sales_return_details, sales_return[['id', 'credit_note_number']], left_on='sales_return_id', right_on='id', how='left')
        sales_return_details.drop(columns=['id_y'], inplace=True)
        sales_return_details.rename(columns={'credit_note_number': 'invoice_number', 'sales_return_id': 'sales_id', 'id_x':'id'}, inplace=True)

        sales_return_details['bill_amount'] = sales_return_details['bill_amount'] * -1
        sales_return_details['quantity'] = sales_return_details['quantity'] * -1

        sales_invoice_details = pd.concat([sales_invoice_details, sales_return_details], ignore_index=True)
        sales_invoice_details = pd.merge(sales_invoice_details, products[['id', 'mis_reporting_category']], left_on='product_id', right_on='id', how='left')
        sales_invoice_details['mis_reporting_category'] = sales_invoice_details['mis_reporting_category'].apply(prod_category)

        sales_invoice_details.drop(columns=['id_y'], inplace=True)
        sales_invoice_details.rename(columns={'id_x': 'id'}, inplace=True)

        sales_invoice_details['bill_amount_generic'] = sales_invoice_details.apply(lambda x: x['bill_amount'] if x['mis_reporting_category'] == 'Generic' else 0, axis=1)

        sales_invoice_details['Month'] = sales_invoice_details['created_at'].apply(lambda x: x.strftime("%B-%y"))


    except Exception as e:
        logger.error(f"Error fetching data from local database: OR Merging Data {e}")
        return None
    
    try:
        month_targets = read_local_data("month_targets")
        

