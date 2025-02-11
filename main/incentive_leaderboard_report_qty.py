from utils.logger import setup_logger
from utils.db_utils import fetch_all_tables, read_local_data, store_data_to_local
from utils.transform_remote import take_requried_columns, make_data_custom_range, add_dates_to_details
from utils.transfom_input import find_replace_from_ip, join_products_bt1, get_incentive_products_bills, add_billing_id

import pandas as pd
import datetime

logger = setup_logger("incentive_leaderboard_report")

def generate_il_report(start_date=None, end_date=None):
    
    # Reading and storing the remote data from the database
    try:
        sales_invoice_details = read_local_data("sales_invoice_details")
        sales_invoices = read_local_data("sales_invoices")
        sales_return = read_local_data("sales_return")
        sales_return_details = read_local_data("sales_return_details")
        products = read_local_data("products")
        stores = read_local_data("stores")
        users = read_local_data("users")
        advance_sales_invoices = read_local_data("advance_sales_invoices")

        sales_invoice_details, sales_return_details = add_dates_to_details(sales_invoices, sales_invoice_details, sales_return, sales_return_details)
        sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = make_data_custom_range(sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices, start_date, end_date)
    except Exception as e:
        logger.error(f"Error fetching data - FROM ILR. {e}")
        return None
    
    
    # Reading and storing the local data from the database
    try:
        brand_tieup_1 = read_local_data("brand_tieup_1")
        brand_tieup_1 = brand_tieup_1.dropna()
        brand_tieup_1['Month'] = brand_tieup_1['Month'].apply(find_replace_from_ip)
    
    except Exception as e:
        logger.error(f"Error fetching local data - FROM ILR. {e}")
        return None
    
    try:
        brand_tieup_1 = join_products_bt1(products, brand_tieup_1)

    except Exception as e:
        logger.error(f"Error joining products with brand_tieup_1 - FROM ILR. {e}")
        return None
    
    try:
        sales_invoice_details = get_incentive_products_bills(sales_invoice_details, brand_tieup_1)
        sales_invoice_details = add_billing_id(sales_invoice_details, sales_invoices)


    except Exception as e:
        logger.error(f"Error getting incentive products bills - FROM ILR. {e}")
        return None
    
    # Now I need to group by the billing_user_id and product_id and sum the bill_amount 
    try:
        sales_invoice_details = sales_invoice_details.groupby(['billing_user_id', 'product_id', 'Month', 'store_id']).agg({'bill_amount': 'sum', 'quantity': 'sum'}).reset_index()
        sales_invoice_details.rename(columns={'product_id': 'ws_code'}, inplace=True)
    
    except Exception as e:
        logger.error(f"Error grouping by billing_user_id and product_id - FROM ILR. {e}")
        return
    
    try:
        final_ilr = pd.merge(sales_invoice_details, brand_tieup_1[['ws_code', 'Incentive', 'Month']], how='left', on=['ws_code', 'Month'])
        final_ilr.rename(columns={'Incentive': 'incentive_per_qty'}, inplace=True)
        final_ilr['total_incentive'] = final_ilr['incentive_per_qty'] * final_ilr['quantity']
        final_ilr.fillna(0, inplace=True)
        # print(final_ilr)

    except Exception as e:
        print()
        logger.error(f"Error adding incentives to the sales_invoice_details - FROM ILR. {e}")
        return
    
    # Now join the products for product name, users for name and stores for the store name
    final_ilr = pd.merge(final_ilr, products[['ws_code', 'product_name']], how='left', on='ws_code')
    
    final_ilr = pd.merge(final_ilr, users[['id', 'name']], how='left', left_on='billing_user_id', right_on='id').drop('id', axis=1)
    final_ilr.rename(columns={'name': 'employee_name'}, inplace=True)
    
    final_ilr = pd.merge(final_ilr, stores[['id', 'name']], how='left', left_on='store_id', right_on='id').drop('id', axis=1)
    final_ilr.rename(columns={'name': 'store_name'}, inplace=True)

    # final_ilr = final_ilr[['store_name', 'employee_name', 'product_name', 'quantity', 'incentive_per_qty', 'total_incentive']]

    # print(final_ilr.head())

    # Saving the data to the local database
    store_data_to_local("incentive_leaderboard_report_qty", final_ilr)
    return final_ilr