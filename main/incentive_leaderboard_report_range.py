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
    
    # Reading and storing the local data from the database in the brand_tieup_2 table
    try:
        brand_tieup_2 = read_local_data("brand_tieup_2")
        brand_tieup_2 = brand_tieup_2.dropna()
        brand_tieup_2['Month'] = brand_tieup_2['Month'].apply(find_replace_from_ip)
        brands = read_local_data("brands")
        brands = brands.dropna()

    except Exception as e:
        logger.error(f"Error fetching local data - FROM ILR. {e}")
        return None
    
    try:
        df_brand_prds = brands.merge(brand_tieup_2, on='brand_cat', how='left')
        df_brand_prds = df_brand_prds.dropna()
        df_brand_prds = df_brand_prds[["product_code", "brand_cat", "Month", "brand_sale_range", "%applied"]]

    except Exception as e:
        logger.error(f"Error joining products with brand_tieup_2 - FROM ILR. {e}")
        return None
    
    # Now we will find all the product_code which are listed in the df_brand_prds
    try:
        df_brand_prds = df_brand_prds.merge(products[['id', 'ws_code']], left_on='product_code', right_on='ws_code', how='left')
        df_brand_prds.to_csv("merged_output.csv", index=False)
        df_brand_prds = df_brand_prds[["id", "brand_cat", "Month", "brand_sale_range", "%applied", "product_code"]]
        df_brand_prds.rename(columns={'id': 'product_id'}, inplace=True)

    except Exception as e:
        logger.error(f"Error joining products with df_brand_prds - FROM ILR. {e}")
        return None
    
    #  Now we will find all the products which are listed in the df_brand_prds
    try:
        listed_prods = df_brand_prds['product_id'].unique().tolist()
        print(listed_prods)

        sales_invoice_details = sales_invoice_details[sales_invoice_details['product_id'].isin(listed_prods)]

        # Add sales_invoices to get the billing_user_id
        sales_invoice_details = add_billing_id(sales_invoice_details, sales_invoices)

        # Now get the product_code from the sales_invoice_details
        sales_invoice_details = sales_invoice_details.merge(products[['id', 'ws_code']], left_on='product_id', right_on='id', how='left')

        sales_invoice_details.drop(columns=['id_y'], inplace=True)

        # Get the brand_cat from the product_code
        sales_invoice_details = sales_invoice_details.merge(brands[['brand_cat', 'product_code']], left_on='ws_code', right_on='product_code', how='left').drop('product_code', axis=1)

        # Now group by the brand_cat and billing_user_id and month and get the sum of the bill_amount and quantity
        sales_invoice_details = sales_invoice_details.groupby(['brand_cat', 'billing_user_id', 'Month', 'store_id']).agg({'bill_amount': 'sum', 'quantity': 'sum'}).reset_index()

        # sales_invoice_details.to_csv("Temp.csv", index=False)

        # Now in the df_brands_prds we have a range which has 2 numbers separated by a hyphen. We will split them and get the upper and lower limit
        df_brand_prds['brand_sale_range'] = df_brand_prds['brand_sale_range'].apply(lambda x: x.split('-'))

        df_brand_prds['lower_limit'] = df_brand_prds['brand_sale_range'].apply(lambda x: float(x[0].strip()))
        df_brand_prds['upper_limit'] = df_brand_prds['brand_sale_range'].apply(lambda x: float(x[1].strip()))

        # Now from df brand prds choose the brand_cat, month, lower_limit, upper_limit and %applied
        df_brand_take = df_brand_prds[['brand_cat', 'Month', 'lower_limit', 'upper_limit', '%applied']]

        df_brand_take = df_brand_take.drop_duplicates()

        # Now merge the sales_invoice_details with the df_brand_take
        sales_invoice_details = sales_invoice_details.merge(df_brand_take, on=['brand_cat', 'Month'], how='left')

        # sales_invoice_details.to_csv("Temp2.csv", index=False)

        # Now we will filter the sales_invoice_details where the bill_amount is between the lower_limit and upper_limit

        sales_invoice_details = sales_invoice_details[(sales_invoice_details['bill_amount'] >= sales_invoice_details['lower_limit']) & (sales_invoice_details['bill_amount'] <= sales_invoice_details['upper_limit'])]

        # Now we will calculate the incentive
        sales_invoice_details['incentive'] = sales_invoice_details['bill_amount'] * sales_invoice_details['%applied'] / 100

        sales_invoice_details.to_csv("Final_Incentive_Range.csv", index=False)

        final_ilrr = pd.merge(sales_invoice_details, users[['id', 'name']], how='left', left_on='billing_user_id', right_on='id').drop('id', axis=1)
        final_ilrr.rename(columns={'name': 'employee_name'}, inplace=True)

        final_ilrr = pd.merge(final_ilrr, stores[['id', 'name']], how='left', left_on='store_id', right_on='id').drop('id', axis=1)
        final_ilrr.rename(columns={'name': 'store_name'}, inplace=True)

        return final_ilrr

    except Exception as e:
        logger.error(f"Error finding the incentive - FROM ILR. {e}")
        return None