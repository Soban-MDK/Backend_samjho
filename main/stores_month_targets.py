from utils.logger import setup_logger
from utils.db_utils import fetch_all_tables, read_local_data
from utils.transform_remote import take_requried_columns, make_data_custom_range, add_dates_to_details, prod_category
from utils.transfom_input import find_replace_from_ip, join_products_bt1, get_incentive_products_bills, add_billing_id

import pandas as pd
import datetime

logger = setup_logger("stores_month_targets")

def generate_stores_month_targets(months_year=None):

    try:
        users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices= fetch_all_tables()
        users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices= take_requried_columns(users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details,advance_sales_invoices)
        sales_invoice_details, sales_return_details = add_dates_to_details(sales_invoices, sales_invoice_details, sales_return,sales_return_details)
        sales_invoice_details['Month'] = sales_invoice_details['created_at'].apply(lambda x: x.strftime("%B-%y"))
        sales_return_details['Month'] = sales_return_details['created_at'].apply(lambda x: x.strftime("%B-%y"))

        # Filter out the data for the given months_year
        if months_year:
            sales_invoice_details = sales_invoice_details[sales_invoice_details['Month'] == months_year]
            sales_return_details = sales_return_details[sales_return_details['Month'] == months_year]


        # Join sales_invoice_details and sales_return_details to sales_invoices and sales_return to get the invoice_number
        sales_invoice_details = pd.merge(sales_invoice_details, sales_invoices[['id', 'invoice_number']], left_on='sales_invoice_id', right_on='id', how='left')
        sales_return_details = pd.merge(sales_return_details, sales_return[['id', 'credit_note_number']], left_on='sales_return_id', right_on='id', how='left')

        # Now rename the column credit_note_number to invoice_number
        sales_return_details.rename(columns={'credit_note_number': 'invoice_number', 'sales_return_id':'sales_id'}, inplace=True)
        sales_invoice_details.rename(columns={'sales_invoice_id':'sales_id'}, inplace=True)
        sales_return_details['bill_amount'] = sales_return_details['bill_amount'] * -1
        sales_return_details['quantity'] = sales_return_details['quantity'] * -1

    except Exception as e:
        logger.error(f"Error fetching data - FROM ILR. {e}")
        return None    
    
    try:
        stores_sales = pd.concat([sales_invoice_details, sales_return_details], ignore_index=True)
        stores_sales = pd.merge(stores_sales, products[['id', 'mis_reporting_category']], left_on='product_id', right_on='id', how='left')
        stores_sales['mis_reporting_category'] = stores_sales['mis_reporting_category'].apply(prod_category)
        # stores_sales.to_csv("stores_sales.csv", index=False)

    except Exception as e:
        logger.error(f"Error converting bill amount to negative - FROM ILR. {e}")
        return None
    
    # Now for each stores, we need to calculate the total sales (poitive), total generic sales, total msp sales and total sales(negative). Note that total msp sales is the sale of the product_id 22421

    try:
        stores_sales['msp_sales'] = stores_sales.apply(lambda x: x['bill_amount'] if x['product_id'] == 22421 else 0, axis=1)
        stores_sales['generic_sales'] = stores_sales.apply(lambda x: x['bill_amount'] if x['mis_reporting_category'] == 'Generic' else 0, axis=1)
        stores_sales['total_sales'] = stores_sales.apply(lambda x: x['bill_amount'] if x['bill_amount'] > 0 else 0, axis=1)
        stores_sales['total_sales_return'] = stores_sales.apply(lambda x: x['bill_amount'] if x['bill_amount'] < 0 else 0, axis=1)

        stores_sales = stores_sales.groupby(['store_id', 'Month']).agg({'total_sales':'sum', 'msp_sales':'sum', 'generic_sales':'sum', 'total_sales_return':'sum'}).reset_index()

        stores_sales.to_csv("stores_sales.csv", index=False)

    except Exception as e:
        logger.error(f"Error calculating total sales - FROM ILR. {e}")
        return None