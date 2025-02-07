from main.incentive_leaderboard_report import generate_il_report
from utils.db_utils import save_csv_to_local
import pandas as pd

save_csv_to_local("./brand_tieup_1.csv")
final_ilr = generate_il_report()