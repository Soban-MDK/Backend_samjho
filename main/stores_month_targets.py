from utils.logger import setup_logger
from utils.db_utils import fetch_all_tables, read_local_data, store_data_to_local
from utils.transform_remote import take_requried_columns, make_data_custom_range, add_dates_to_details, prod_category
from utils.transfom_input import find_replace_from_ip, join_products_bt1, get_incentive_products_bills, add_billing_id

import pandas as pd
import datetime

logger = setup_logger("stores_month_targets")

def generate_stores_month_targets(months_year=None):

    try:
        sales_invoice_details = read_local_data("sales_invoice_details")
        sales_invoices = read_local_data("sales_invoices")
        sales_return = read_local_data("sales_return")
        sales_return_details = read_local_data("sales_return_details")
        products = read_local_data("products")
        stores = read_local_data("stores")

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

        stores_sales = pd.merge(stores_sales, stores[['id', 'name']], left_on='store_id', right_on='id', how='left')
        months_target = read_local_data("month_targets")
        months_target['Month'] = months_target['Month'].apply(find_replace_from_ip)

        print(months_target.head())


    except Exception as e:
        logger.error(f"Error calculating total sales - FROM ILR. {e}")
        return None
    
    try:
        final_report = pd.merge(stores_sales, months_target, left_on=['name', 'Month'], right_on=['StoreName', 'Month'], how='left')
        final_report.drop(columns=['StoreName'], inplace=True)
        final_report.to_csv("stores_sales.csv", index=False)

    except Exception as e:
        logger.error(f"Error merging the data - FROM ILR. {e}")
        return None
    
    try:
        # Now find the effective sales which would be the sum of total_sales and total_sales_return
        final_report['effective_sales'] = final_report['total_sales'] + final_report['total_sales_return']

        # If the Store, Generic or MSP is null then add Sales% as 100, Generic% as 100 and MSP% as 100
        final_report = final_report.fillna(0)
        final_report["Sales%"] = 0
        final_report["Generic%"] = 0
        final_report["MSP%"] = 0

        final_report.to_csv("final_report.csv")
        print("Done")
        
        final_report['Sales%'] = 100  # Default value
        mask = final_report['Store'] != 0
        final_report.loc[mask, 'Sales%'] = (final_report.loc[mask, 'effective_sales'] / final_report.loc[mask, 'Store']) * 100

        # Handle Generic%
        final_report['Generic%'] = 100  # Default value
        mask = final_report['Generic'] != 0
        final_report.loc[mask, 'Generic%'] = (final_report.loc[mask, 'generic_sales'] / final_report.loc[mask, 'Generic']) * 100

        # Handle MSP%
        final_report['MSP%'] = 100  # Default value
        mask = final_report['MSP'] != 0
        final_report.loc[mask, 'MSP%'] = (final_report.loc[mask, 'msp_sales'] / final_report.loc[mask, 'MSP']) * 100

        # If the values in MSP%, Sales% and Generic% are all 100 then make all of them to 0
        mask = (final_report['MSP%'] == 100) & (final_report['Sales%'] == 100) & (final_report['Generic%'] == 100)
        final_report.loc[mask, 'MSP%'] = 0
        final_report.loc[mask, 'Sales%'] = 0
        final_report.loc[mask, 'Generic%'] = 0

        # add a column which will be true if all the sales report are greater than or equal to 100% else False
        final_report['Achieved'] = (final_report['MSP%'] >= 100) & (final_report['Sales%'] >= 100) & (final_report['Generic%'] >= 100)

        store_data_to_local("sales_target_report", final_report)
        return final_report
    
    except Exception as e:
        logger.error(f"Error calculating the percentages - FROM ILR. {e}")
        return None
        