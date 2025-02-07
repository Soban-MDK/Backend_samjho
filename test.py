from utils.db_utils import fetch_all_tables
from utils.transform_local import take_requried_columns


# Fetch all the tables
users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = fetch_all_tables()

# Take only the required columns
users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = take_requried_columns(users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices)

# Print the first 5 rows of the products table
print(products.head())