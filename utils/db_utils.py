from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from config.config import *
from logger import setup_logger

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
            "sales_invoice_details", "sales_returns", 
            "sales_return_details", "advanced_sales_invoices"
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
        sales_returns = data_frames["sales_returns"]
        sales_return_details = data_frames["sales_return_details"]
        advanced_sales_invoices = data_frames["advanced_sales_invoices"]

        logger.info("Fetched all 8 tables successfully.")

        return users, products, stores, sales_invoices, sales_invoice_details, sales_returns, sales_return_details, advanced_sales_invoices

    except Exception as e:
        logger.error(f"Error fetching all tables: {e}")
        return None


# Store transformed data into local DB
def store_transformed_data(table_name, df):
    try:
        engine = get_local_engine()
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logger.info(f"Stored transformed data into {table_name}.")
    except Exception as e:
        logger.error(f"Error storing data into {table_name}: {e}")
