import pandas as pd
import datetime
from utils.db_utils import fetch_all_tables, save_csv_to_local, read_local_data
from utils.transform_remote import take_requried_columns, make_data_custom_range, add_dates_to_details

# VARIABLES
start_date = None
end_date = None


# Fetch all the tables
users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = fetch_all_tables()


# Take only the required columns
users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = take_requried_columns(users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices)


# Add dates to details
sales_invoice_details, sales_return_details = add_dates_to_details(sales_invoices, sales_invoice_details, sales_return, sales_return_details)

sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = make_data_custom_range(sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices, start_date, end_date)

sales_invoice_details.to_csv("sales_invoice_details.csv", index=False)

# Read data from local DB
brand_tieup_1 = read_local_data("brand_tieup_1")
print(brand_tieup_1.head())