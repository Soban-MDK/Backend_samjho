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
    
     