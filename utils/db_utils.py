from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from config.config import *
from utils.logger import setup_logger

logger = setup_logger("database")

# Remote DB Engine
def get_remote_engine():
    try:
        engine = create_engine(f"postgresql://{REMOTE_DB_USER}:{REMOTE_DB_PASS}@{REMOTE_DB_HOST}:{REMOTE_DB_PORT}/{REMOTE_DB_NAME}")
        logger.info("Connected to remote database.")
        return engine
    except Exception as e:
        logger.error(f"Remote DB connection failed: {e}")
        raise

# Local DB Engine
def get_local_engine():
    try:
        engine = create_engine(f"postgresql://{LOCAL_DB_USER}:{LOCAL_DB_PASS}@{LOCAL_DB_HOST}:{LOCAL_DB_PORT}/{LOCAL_DB_NAME}")
        logger.info("Connected to local database.")
        return engine
    except Exception as e:
        logger.error(f"Local DB connection failed: {e}")
        raise

# Fetch all tables in a single connection from the remote DB
def fetch_all_tables():
    try:
        engine = get_remote_engine()
        table_names = [
            "users", "products", "stores", "sales_invoices", 
            "sales_invoice_details", "sales_return", 
            "sales_return_details", "advance_sales_invoices"
        ]
        
        with engine.connect() as connection:
            data_frames = {
                table: pd.read_sql_query(f"SELECT * FROM {table};", connection)
                for table in table_names
            }

        # Storing data in separate variables
        users = data_frames["users"]
        products = data_frames["products"]
        stores = data_frames["stores"]
        sales_invoices = data_frames["sales_invoices"]
        sales_invoice_details = data_frames["sales_invoice_details"]
        sales_return = data_frames["sales_return"]
        sales_return_details = data_frames["sales_return_details"]
        advance_sales_invoices = data_frames["advance_sales_invoices"]

        logger.info("Fetched all 8 tables successfully.")

        return users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices

    except Exception as e:
        logger.error(f"Error fetching all tables: {e}")
        return None


# Read data from local DB
def read_local_data(table_name):
    try:
        engine = get_local_engine()
        with engine.connect() as connection:
            df = pd.read_sql_table(table_name, connection)
        logger.info(f"Read {len(df)} data from {table_name}.")
        return df
    
    except Exception as e:
        logger.error(f"Error reading data from {table_name}: {e}")
        return None


def save_csv_to_local(file_path):
    try:
        engine = get_local_engine()
        table_name = os.path.splitext(os.path.basename(file_path))[0]  # Extract table name from filename

        # Read CSV file
        df = pd.read_csv(file_path)

        with engine.connect() as connection:
            # Create table if not exists
            df.head(0).to_sql(table_name, con=engine, if_exists='append', index=False)  
            
            # Get existing data (assuming the table has a primary key 'id' or composite key logic)
            existing_df = pd.read_sql_query(f"SELECT * FROM {table_name}", connection)

            # Identify new rows by comparing the full dataframe
            new_data = df.merge(existing_df, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

            if not new_data.empty:
                new_data.to_sql(table_name, con=engine, if_exists='append', index=False)
                logger.info(f"Inserted {len(new_data)} new rows into {table_name}.")
            else:
                logger.info("No new data to insert. Skipping insertion.")

    except Exception as e:
        logger.error(f"Error processing CSV: {e}")


# Store transformed data into local DB
def store_data_to_local(table_name, df):
    try:
        engine = get_local_engine()
        df.to_sql(table_name, engine, if_exists="append", index=False)
        logger.info(f"Stored transformed data into {table_name}.")
    except Exception as e:
        logger.error(f"Error storing data into {table_name}: {e}")
