from main.incentive_leaderboard_report_qty import generate_il_report
from main.incentive_leaderboard_report_range import generate_il_report_range
from main.stores_month_targets import generate_stores_month_targets
from main.spot_targets_reports import generate_stores_spot_targets


from utils.db_utils import save_csv_to_local, store_data_to_local, fetch_all_tables
from utils.transform_remote import take_requried_columns
import pandas as pd



# Saving the required data to local database
# users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices= fetch_all_tables()
# users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details, advance_sales_invoices= take_requried_columns(users, products, stores, sales_invoices, sales_invoice_details, sales_return, sales_return_details,advance_sales_invoices)
# store_data_to_local("users", users)
# store_data_to_local("products", products)
# store_data_to_local("stores", stores)
# store_data_to_local("sales_invoices", sales_invoices)
# store_data_to_local("sales_invoice_details", sales_invoice_details)
# store_data_to_local("sales_return", sales_return)
# store_data_to_local("sales_return_details", sales_return_details)
# store_data_to_local("advance_sales_invoices", advance_sales_invoices)
# print("Data stored in local database")


## REPORT 1
# save_csv_to_local("./brand_tieup_1.csv")
# final_ilr = generate_il_report()
# store_data_to_local("incentive_leaderboard_report_qty", final_ilr)


## REPORT 2
## Adding the data to the local database
# save_csv_to_local("./brand_tieup_2.csv")
# save_csv_to_local("./brands.csv")

# final_ilr = generate_il_report_range()
# store_data_to_local("incentive_leaderboard_report_range", final_ilr)


## REPORT 3
# save_csv_to_local("./month_targets.csv")
# test = generate_stores_month_targets()
# store_data_to_local("sales_target_report", test)


### REPORT 4

# Fetching data from remote and then storing it in local
save_csv_to_local("./spot_targets.csv")
generate_stores_spot_targets()