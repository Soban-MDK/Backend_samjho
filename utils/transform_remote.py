import pandas as pd
from utils.logger import setup_logger

logger = setup_logger("transform_remote")

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


def make_data_custom_range(sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices, start_date=None, end_date=None):
    try:
        sales_invoices["created_at"] = pd.to_datetime(sales_invoices["created_at"])
        sales_return["created_at"] = pd.to_datetime(sales_return["created_at"])
        advance_sales_invoices["created_at"] = pd.to_datetime(advance_sales_invoices["created_at"])
        sales_invoice_details["created_at"] = pd.to_datetime(sales_invoice_details["created_at"])
        sales_return_details["created_at"] = pd.to_datetime(sales_return_details["created_at"])

        if start_date and end_date:
        # Convert to datetime without formatting to string
            sales_invoices = sales_invoices[(sales_invoices["created_at"] >= start_date) & (sales_invoices["created_at"] <= end_date)]
            sales_return = sales_return[(sales_return["created_at"] >= start_date) & (sales_return["created_at"] <= end_date)]
            advance_sales_invoices = advance_sales_invoices[(advance_sales_invoices["created_at"] >= start_date) & (advance_sales_invoices["created_at"] <= end_date)]
            sales_invoice_details = sales_invoice_details[(sales_invoice_details["created_at"] >= start_date) & (sales_invoice_details["created_at"] <= end_date)]
            sales_return_details = sales_return_details[(sales_return_details["created_at"] >= start_date) & (sales_return_details["created_at"] <= end_date)]

        sales_invoices["Month"] = sales_invoices["created_at"].apply(lambda x: x.strftime("%B-%y"))
        sales_return["Month"] = sales_return["created_at"].apply(lambda x: x.strftime("%B-%y"))
        advance_sales_invoices["Month"] = advance_sales_invoices["created_at"].apply(lambda x: x.strftime("%B-%y"))
        sales_invoice_details["Month"] = sales_invoice_details["created_at"].apply(lambda x: x.strftime("%B-%y"))
        sales_return_details["Month"] = sales_return_details["created_at"].apply(lambda x: x.strftime("%B-%y"))

        print("Made data custom ranged")
        logger.info("Made data custom ranged")
    
    except Exception as e:
        print("Error occured : ", e)
        logger.error(f"Error making data custom ranged: {e}")
    
    return sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices


def add_dates_to_details(sales_invoices, sales_invoice_details, sales_return, sales_return_details):
    try:
        sales_invoice_details = pd.merge(sales_invoice_details, sales_invoices[["id", "created_at"]], left_on="sales_invoice_id", right_on="id", how="left")
        sales_invoice_details = sales_invoice_details.drop(columns=["id_y"])
        sales_invoice_details = sales_invoice_details.rename(columns={"id_x": "id"})

        sales_return_details = pd.merge(sales_return_details, sales_return[["id", "created_at"]], left_on="sales_return_id", right_on="id", how="left")
        sales_return_details = sales_return_details.drop(columns=["id_y"])
        sales_return_details = sales_return_details.rename(columns={"id_x": "id"})

        logger.info("Added dates to details successfully")

    except Exception as e:
        logger.error(f"Error adding dates to details: {e}")

    return sales_invoice_details, sales_return_details

def prod_category(mis_reporting_category):
    if "branded" in mis_reporting_category.lower():
        return "Branded"
    
    else:
        return "Generic"
    
