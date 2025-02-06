from utils.db_utils import fetch_all_tables

users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = fetch_all_tables()

print(users.head())