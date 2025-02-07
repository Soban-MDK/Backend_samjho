import pandas as pd
from utils.logger import setup_logger

logger = setup_logger("transform")

# Now taking only the required columns from the fetched tables
def take_requried_columns(users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices):
    try:
        users = users[["id", "name", "mobile_number", "is_active", "mre_user_id"]]
        products = products[["id", "product_name", "ws_code", "mis_reporting_category", "mrp", "sales_price"]]
        stores = stores[["id", "name", "store_type"]]
        sales_invoices = sales_invoices[["id", "store_id", "billing_user_id", "total_products", "total_quantity", "total_bill_amount", "prepaid_amount", "invoice_number", "created_at"]]
        sales_invoice_details = sales_invoice_details[["id", "bill_amount", "quantity", "sales_invoice_id", "product_id", "store_id"]]
        sales_return = sales_return[["id", "total_products", "total_quantity", "bill_amount", "credit_note_number", "created_at"]]
        sales_return_details = sales_return_details[["id", "bill_amount", "quantity", "sales_return_id", "product_id", "store_id"]]
        advance_sales_invoices = advance_sales_invoices[["id", "total_products", "total_quantity", "total_amount", "prepaid_amount", "invoice_number", "status","is_urgent_order", "created_at", "store_id", "billing_user_id"]]
    
        logger.info("Took required columns from all tables")

    except Exception as e:
        logger.error(f"Error taking required columns from all tables: {e}")

    return users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices


