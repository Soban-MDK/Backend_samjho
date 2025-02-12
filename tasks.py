from celery_config import celery
from celery import chain
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

from utils.db_utils import fetch_all_tables, store_data_to_local
from utils.transform_remote import take_requried_columns

from main.advanced_urgent_reports import generate_au_reports
from main.incentive_leaderboard_report_qty import generate_il_report
from main.incentive_leaderboard_report_range import generate_il_report_range
from main.spot_targets_reports import generate_stores_spot_targets_daily
from main.stores_month_targets import generate_stores_month_targets
from main.wow_reports import generate_wow_reports
from main.zero_brand_sales import generate_zero_brand_report


@celery.task(name="tasks.fetch_and_store_data", bind=True, max_retries=3, default_retry_delay=300)
def fetch_and_store_data(self):
    try:
        logger.info("Fetching data...")
        data = fetch_all_tables()
        print("Fetched tables:", data) 
        if len(data) != 8:
            raise ValueError(f"Expected 8 tables, but got {len(data)}: {data}")

        users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = data

        users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices = take_requried_columns(users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices)

        store_data_to_local("users", users)
        store_data_to_local("products", products)
        store_data_to_local("stores", stores)
        store_data_to_local("sales_invoices", sales_invoices)
        store_data_to_local("sales_invoice_details", sales_invoice_details)
        store_data_to_local("sales_return", sales_return)
        store_data_to_local("sales_return_details", sales_return_details)
        store_data_to_local("advance_sales_invoices", advance_sales_invoices)

        logger.info("Data fetched and stored successfully")
        return "fetch_and_store_data_completed"
    except Exception as exc:
        logger.error(f"Error in fetch_and_store_data: {str(exc)}")
        self.retry(exc=exc)


@celery.task(
    name="tasks.generate_reports",
    bind=True,
    max_retries=3,
    default_retry_delay=300
)
def generate_reports(self, previous_task_result=None):
    try:
        logger.info("Generating reports...")
        print("Generating reports...")
        au_report = generate_au_reports()
        il_qty = generate_il_report()
        il_range = generate_il_report_range()
        spot = generate_stores_spot_targets_daily()

        store_month_targets = generate_stores_month_targets()
        wow_report = generate_wow_reports()
        zero_brand_report = generate_zero_brand_report()
        logger.info("All reports generated successfully")
        return "Report generation completed"
    except Exception as exc:
        logger.error(f"Error in generate_reports: {str(exc)}")
        self.retry(exc=exc)

@celery.task(name="tasks.full_pipeline")
def full_pipeline():
    """
    Creates a chain of tasks to be executed sequentially.
    """
    try:
        logger.info("Starting full pipeline execution")
        chain_workflow = chain(
            fetch_and_store_data.s(),
            generate_reports.s()
        )
        result = chain_workflow.apply_async()
        return result.id
    except Exception as exc:
        logger.error(f"Error in full pipeline: {exc}")
        raise