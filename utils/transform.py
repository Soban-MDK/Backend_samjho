import pandas as pd
from utils.logger import setup_logger

logger = setup_logger("transform")

def transform_data(df, table_name):
    try:
        if table_name == "users":
            df["email"] = df["email"].str.lower()
        elif table_name == "products":
            df["price"] = df["price"] * 1.1  # Example transformation: Increase price by 10%

        logger.info(f"Transformed data for {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error transforming data for {table_name}: {e}")
        return df
