from main.incentive_leaderboard_report_qty import generate_il_report
from main.incentive_leaderboard_report_range import generate_il_report_range
from utils.db_utils import save_csv_to_local, store_data_to_local
import pandas as pd

# save_csv_to_local("./brand_tieup_1.csv")
# final_ilr = generate_il_report()
# store_data_to_local("incentive_leaderboard_report_qty", final_ilr)

# Adding the data to the local database
save_csv_to_local("./brand_tieup_2.csv")
save_csv_to_local("./brands.csv")

final_ilr = generate_il_report_range()
# final_ilr.to_csv("final_ilr.csv", index=False)