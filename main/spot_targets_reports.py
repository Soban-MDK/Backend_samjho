from utils.logger import setup_logger
from utils.db_utils import fetch_all_tables, read_local_data, store_data_to_local
from utils.transform_remote import take_requried_columns, make_data_custom_range, add_dates_to_details, prod_category
from utils.transfom_input import find_replace_from_ip, join_products_bt1, get_incentive_products_bills, add_billing_id

import pandas as pd
import datetime

logger = setup_logger("stores_month_targets")

def generate_stores_spot_targets(months_year=None):

    try:
        sales_invoice_details = read_local_data("sales_invoice_details")
        sales_invoices = read_local_data("sales_invoices")
        sales_return = read_local_data("sales_return")
        sales_return_details = read_local_data("sales_return_details")
        products = read_local_data("products")
        stores = read_local_data("stores")
        users = read_local_data("users")
        
        sales_invoice_details, sales_return_details = add_dates_to_details(sales_invoices, sales_invoice_details, sales_return, sales_return_details)
        sales_invoice_details = pd.merge(sales_invoice_details, sales_invoices[['id', 'invoice_number']], left_on='sales_invoice_id', right_on='id', how='left')
        sales_invoice_details.drop(columns=['id_y'], inplace=True)
        sales_invoice_details.rename(columns={'sales_invoice_id': 'sales_id', 'id_x': 'id'}, inplace=True)


        sales_return_details = pd.merge(sales_return_details, sales_return[['id', 'credit_note_number']], left_on='sales_return_id', right_on='id', how='left')
        sales_return_details.drop(columns=['id_y'], inplace=True)
        sales_return_details.rename(columns={'credit_note_number': 'invoice_number', 'sales_return_id': 'sales_id', 'id_x':'id'}, inplace=True)

        sales_return_details['bill_amount'] = sales_return_details['bill_amount'] * -1
        sales_return_details['quantity'] = sales_return_details['quantity'] * -1

        sales_invoice_details = pd.concat([sales_invoice_details, sales_return_details], ignore_index=True)
        sales_invoice_details = pd.merge(sales_invoice_details, products[['id', 'mis_reporting_category']], left_on='product_id', right_on='id', how='left')
        sales_invoice_details['mis_reporting_category'] = sales_invoice_details['mis_reporting_category'].apply(prod_category)

        sales_invoice_details.drop(columns=['id_y'], inplace=True)
        sales_invoice_details.rename(columns={'id_x': 'id'}, inplace=True)

        # print(sales_invoice_details.head())

    except Exception as e:
        logger.error(f"Error fetching data from local database: OR Merging Data {e}")
        return None

    # Here reading and adjusting the data of spot_targets
    try:
        spot_targets = read_local_data("spot_targets")
        
        # Split the Date column in to two dates upper and lower which can be separated by '-' in the date column
        spot_targets['Date'] = spot_targets['Date'].apply(lambda x: x.split('-'))
        spot_targets['Date_upper'] = spot_targets['Date'].apply(lambda x: x[1])
        spot_targets['Date_lower'] = spot_targets['Date'].apply(lambda x: x[0])
        spot_targets['Date_upper'] = pd.to_datetime(spot_targets['Date_upper'])
        spot_targets['Date_lower'] = pd.to_datetime(spot_targets['Date_lower'])

        spot_targets.drop(columns=['Date'], inplace=True)

    except Exception as e:
        logger.error(f"Error fetching data from local database: OR Merging Data {e}")
        return None
    
    # Now merge the data of spot_targets with sales_invoice_details
    try:
        sales_invoice_details = pd.merge(sales_invoice_details, stores[['id', 'name']], left_on='store_id', right_on='id', how='left')
        sales_invoice_details.drop(columns='id_y', inplace=True)
        sales_invoice_details.rename(columns={'id_x': 'id', 'name':'store_name'}, inplace=True)
        
        sales_invoice_details = pd.merge(sales_invoice_details, spot_targets, left_on='store_name', right_on='StoreName', how='left')

        # Drop all the rows where Store name is empty or store_name not equals to Storename
        sales_invoice_details.dropna(subset=['StoreName'], inplace=True)
        sales_invoice_details = sales_invoice_details[sales_invoice_details['store_name'] == sales_invoice_details['StoreName']]

        # Drop the columns which are not required
        sales_invoice_details.drop(columns='StoreName', inplace=True)

        # Now take only those rows where the created_at is between the Date_lower and Date_upper
        sales_invoice_details = sales_invoice_details[(sales_invoice_details['created_at'] >= sales_invoice_details['Date_lower']) & (sales_invoice_details['created_at'] <= sales_invoice_details['Date_upper'])]

    except Exception as e:
        logger.error(f"Error merging data of spot_targets with sales_invoice_details {e}")
        return None
    
    try:
        # Now add a column for branded sales and set it to bill_amount if the mis_reporting_category is not 'Generic' else set it to 0
        sales_invoice_details['generic_sales'] = sales_invoice_details.apply(lambda x: x['bill_amount'] if x['mis_reporting_category'] == 'Generic' else 0, axis=1)

        sales_invoice_details['positive_sales'] = sales_invoice_details['bill_amount'].apply(lambda x: x if x > 0 else 0)
        sales_invoice_details['negative_sales'] = sales_invoice_details['bill_amount'].apply(lambda x: x if x < 0 else 0)

        sales_invoice_details['generic_positive'] = sales_invoice_details['generic_sales'].apply(lambda x: x if x > 0 else 0)
        sales_invoice_details['generic_negative'] = sales_invoice_details['generic_sales'].apply(lambda x: x if x < 0 else 0)
        # sales_invoice_details.drop(columns='generic_sales', inplace=True)

        # Now group by
        sales_invoice_details = sales_invoice_details.groupby(['store_name', 'Date_lower', 'Date_upper', 'SpotTarget', 'genSpotTarget']).agg({'bill_amount':'sum', 'generic_positive':'sum', 'generic_negative':'sum', 'positive_sales':'sum', 'negative_sales':'sum'}).reset_index()

        # Achieved should be true if bill_amount is greater than or equal to SpotTarget and the generic_sales is greater than or equal to genSpotTarget
        sales_invoice_details['Achieved'] = sales_invoice_details.apply(lambda x: True if x['bill_amount'] >= x['SpotTarget'] and x['generic_positive'] >= x['genSpotTarget'] else False, axis=1)

        store_data_to_local("stores_spot_targets", sales_invoice_details)
        return sales_invoice_details

    except Exception as e:
        logger.error(f"Error adding generic sales column {e}")
        return None
    

def generate_stores_spot_targets_daily(months_year=None):

    try:
        sales_invoice_details = read_local_data("sales_invoice_details")
        sales_invoices = read_local_data("sales_invoices")
        sales_return = read_local_data("sales_return")
        sales_return_details = read_local_data("sales_return_details")
        products = read_local_data("products")
        stores = read_local_data("stores")
        users = read_local_data("users")
        
        sales_invoice_details, sales_return_details = add_dates_to_details(sales_invoices, sales_invoice_details, sales_return, sales_return_details)
        sales_invoice_details = pd.merge(sales_invoice_details, sales_invoices[['id', 'invoice_number']], left_on='sales_invoice_id', right_on='id', how='left')
        sales_invoice_details.drop(columns=['id_y'], inplace=True)
        sales_invoice_details.rename(columns={'sales_invoice_id': 'sales_id', 'id_x': 'id'}, inplace=True)


        sales_return_details = pd.merge(sales_return_details, sales_return[['id', 'credit_note_number']], left_on='sales_return_id', right_on='id', how='left')
        sales_return_details.drop(columns=['id_y'], inplace=True)
        sales_return_details.rename(columns={'credit_note_number': 'invoice_number', 'sales_return_id': 'sales_id', 'id_x':'id'}, inplace=True)

        sales_return_details['bill_amount'] = sales_return_details['bill_amount'] * -1
        sales_return_details['quantity'] = sales_return_details['quantity'] * -1

        sales_invoice_details = pd.concat([sales_invoice_details, sales_return_details], ignore_index=True)
        sales_invoice_details = pd.merge(sales_invoice_details, products[['id', 'mis_reporting_category']], left_on='product_id', right_on='id', how='left')
        sales_invoice_details['mis_reporting_category'] = sales_invoice_details['mis_reporting_category'].apply(prod_category)

        sales_invoice_details.drop(columns=['id_y'], inplace=True)
        sales_invoice_details.rename(columns={'id_x': 'id'}, inplace=True)

        # print(sales_invoice_details.head())

    except Exception as e:
        logger.error(f"Error fetching data from local database: OR Merging Data {e}")
        return None

    # Here reading and adjusting the data of spot_targets
    try:
        spot_targets = read_local_data("spot_targets")
        
        # Split the date ranges and create daily targets
        daily_targets = []
        
        for _, row in spot_targets.iterrows():
            date_range = row['Date'].split('-')
            start_date = pd.to_datetime(date_range[0])
            end_date = pd.to_datetime(date_range[1])
            
            # Calculate number of days in the range (inclusive)
            days_diff = (end_date - start_date).days + 1
            
            # Calculate daily targets
            daily_spot_target = row['SpotTarget'] / days_diff
            daily_gen_spot_target = row['genSpotTarget'] / days_diff
            
            # Generate a row for each date in the range
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            for date in date_range:
                daily_targets.append({
                    'StoreName': row['StoreName'],
                    'Date': date,
                    'SpotTarget': daily_spot_target,
                    'genSpotTarget': daily_gen_spot_target,
                    'OriginalDateRange': row['Date']
                })
        
        # Convert to DataFrame
        daily_spot_targets = pd.DataFrame(daily_targets)
        print(daily_spot_targets)
        
    except Exception as e:
        logger.error(f"Error fetching data from local database: OR Merging Data {e}")
        return None
    
    sales_invoice_details = pd.merge(sales_invoice_details, stores[['id', 'name']], left_on='store_id', right_on='id', how='left')
    sales_invoice_details.drop(columns='id_y', inplace=True)
    sales_invoice_details.rename(columns={'id_x': 'id', 'name':'store_name'}, inplace=True)

    # Convert to yyyy-mm-dd format
    sales_invoice_details['created_at'] = sales_invoice_details['created_at'].dt.date
    daily_spot_targets['Date'] = pd.to_datetime(daily_spot_targets['Date']).dt.date

    # Merge
    sales_invoice_details = pd.merge(sales_invoice_details, daily_spot_targets, left_on=['store_name', 'created_at'], right_on=
    ['StoreName', 'Date'], how='left')

    sales_invoice_details = sales_invoice_details[sales_invoice_details['store_name'] == sales_invoice_details['StoreName']]
    sales_invoice_details.drop(columns='StoreName', inplace=True)

    sales_invoice_details = sales_invoice_details[sales_invoice_details['created_at'] == sales_invoice_details['Date']]
    sales_invoice_details.drop(columns='Date', inplace=True)


    sales_invoice_details['generic_sales'] = sales_invoice_details.apply(lambda x: x['bill_amount'] if x['mis_reporting_category'] == 'Generic' else 0, axis=1)

    sales_invoice_details['positive_sales'] = sales_invoice_details['bill_amount'].apply(lambda x: x if x > 0 else 0)
    sales_invoice_details['negative_sales'] = sales_invoice_details['bill_amount'].apply(lambda x: x if x < 0 else 0)

    sales_invoice_details['generic_positive'] = sales_invoice_details['generic_sales'].apply(lambda x: x if x > 0 else 0)
    sales_invoice_details['generic_negative'] = sales_invoice_details['generic_sales'].apply(lambda x: x if x < 0 else 0)

    print(sales_invoice_details.columns)

    sales_invoice_details = sales_invoice_details.groupby(['store_name', 'created_at', 'SpotTarget', 'genSpotTarget', 'OriginalDateRange']).agg({'bill_amount':'sum', 'generic_positive':'sum', 'generic_negative':'sum', 'positive_sales':'sum', 'negative_sales':'sum'}).reset_index()

    sales_invoice_details['Achieved'] = sales_invoice_details.apply(lambda x: True if x['bill_amount'] >= x['SpotTarget'] and x['generic_positive'] >= x['genSpotTarget'] else False, axis=1)

    overall_achieved = sales_invoice_details.groupby(['store_name', 'OriginalDateRange']).agg({'SpotTarget': 'sum', 'genSpotTarget': 'sum', 'bill_amount': 'sum', 'generic_positive': 'sum', 'generic_negative': 'sum', 'positive_sales': 'sum', 'negative_sales': 'sum'}).reset_index()

    # Round off to the nearest integers these values
    overall_achieved['SpotTarget'] = overall_achieved['SpotTarget'].apply(lambda x: round(x))
    overall_achieved['genSpotTarget'] = overall_achieved['genSpotTarget'].apply(lambda x: round(x))
    overall_achieved['bill_amount'] = overall_achieved['bill_amount'].apply(lambda x: round(x))
    overall_achieved['generic_positive'] = overall_achieved['generic_positive'].apply(lambda x: round(x))   
    overall_achieved['generic_negative'] = overall_achieved['generic_negative'].apply(lambda x: round(x))
    overall_achieved['positive_sales'] = overall_achieved['positive_sales'].apply(lambda x: round(x))
    overall_achieved['negative_sales'] = overall_achieved['negative_sales'].apply(lambda x: round(x))

    # Now check the condition of achieved
    overall_achieved['overall_Achieved'] = overall_achieved.apply(lambda x: True if x['bill_amount'] >= x['SpotTarget'] and x['generic_positive'] >= x['genSpotTarget'] else False, axis=1)

    # join the two dataframes
    sales_invoice_details = pd.merge(sales_invoice_details, overall_achieved[['store_name', 'OriginalDateRange', 'overall_Achieved']], left_on=['store_name', 'OriginalDateRange'], right_on=['store_name', 'OriginalDateRange'], how='left')

    store_data_to_local("stores_spot_targets", sales_invoice_details)
    return sales_invoice_details