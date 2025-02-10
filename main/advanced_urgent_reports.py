from utils.logger import setup_logger
from utils.db_utils import fetch_all_tables, read_local_data
from utils.transform_remote import take_requried_columns, make_data_custom_range, add_dates_to_details
from utils.transfom_input import find_replace_from_ip, join_products_bt1, get_incentive_products_bills, add_billing_id

import pandas as pd
import datetime

logger = setup_logger("incentive_leaderboard_report")

def generate_au_reports():
    try:
        advanced_urgent_orders = read_local_data("advance_sales_invoices")
        stores = read_local_data("stores")
        users = read_local_data("users")

        # Set the format to yyyy-mm-dd
        advanced_urgent_orders['created_at'] = pd.to_datetime(advanced_urgent_orders['created_at']).dt.strftime('%Y-%m-%d')
        # print(advanced_urgent_orders.head())

        # Divide the total amount in two parts 1) Urgent amount and 2) Advanced amount
        # If the is_urgent_order is true then the entire amount is urgent amount else the entire amount is advanced amount
        advanced_urgent_orders['urgent_amount'] = advanced_urgent_orders.apply(lambda x: x['total_amount'] if x['is_urgent_order'] == True else 0, axis=1)
        advanced_urgent_orders['advanced_amount'] = advanced_urgent_orders.apply(lambda x: x['total_amount'] if x['is_urgent_order'] == False else 0, axis=1)

        # Now select all the rows that have status either pending or invoiced
        advanced_urgent_orders = advanced_urgent_orders.loc[advanced_urgent_orders['status'].isin(['PENDING', 'INVOICED'])]

        advanced_urgent_orders['urgent_invoiced'] = advanced_urgent_orders.apply(lambda x: x['total_amount'] if x['is_urgent_order'] == True and x['status'] == 'INVOICED' else 0, axis=1)

        advanced_urgent_orders['advanced_invoiced'] = advanced_urgent_orders.apply(lambda x: x['total_amount'] if x['is_urgent_order'] == False and x['status'] == 'INVOICED' else 0, axis=1)

        advanced_urgent_orders.rename(columns={'urgent_amount': 'urgent_punched', 'advanced_amount': 'advanced_punched'}, inplace=True)

        advanced_urgent_orders = advanced_urgent_orders.groupby(['store_id', 'created_at', 'billing_user_id']).agg({'urgent_punched': 'sum', 'advanced_punched': 'sum', 'urgent_invoiced': 'sum', 'advanced_invoiced': 'sum'}).reset_index()
        
        advanced_urgent_orders = pd.merge(advanced_urgent_orders, stores[['id', 'name']], left_on='store_id', right_on='id', how='left')
        advanced_urgent_orders.drop(columns='id', inplace=True)
        advanced_urgent_orders.rename(columns={'name':'store_name'}, inplace=True)

        advanced_urgent_orders = pd.merge(advanced_urgent_orders, users[['id', 'name']], left_on='billing_user_id', right_on='id', how='left')
        advanced_urgent_orders.drop(columns='id', inplace=True)
        advanced_urgent_orders.rename(columns={'id_x': 'id', 'name':'employee_name'}, inplace=True)

        # Urgent% is the ratio of urgent_invoiced to urgent_punched and Advanced% is the ratio of advanced_invoiced to advanced_punched but the condition is that the denominator should not be zero. If the denominator is zero then the value should be zero
        advanced_urgent_orders['urgent%'] = advanced_urgent_orders.apply(lambda x: x['urgent_invoiced']/x['urgent_punched']*100 if x['urgent_punched'] != 0 else 0, axis=1)

        advanced_urgent_orders['advanced%'] = advanced_urgent_orders.apply(lambda x: x['advanced_invoiced']/x['advanced_punched']*100 if x['advanced_punched'] != 0 else 0, axis=1)

        return advanced_urgent_orders

    except Exception as e:
        logger.error(f"Error in generating advanced urgent reports: {str(e)}")
        return pd.DataFrame()