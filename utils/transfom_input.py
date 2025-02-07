import os
from utils.logger import setup_logger
from utils.db_utils import get_local_engine

logger = setup_logger("transform_input")

def find_replace_from_ip(months_year):
    find_replace = {
        "Jan": "January",
        "Feb": "February",
        "Mar": "March",
        "Apr": "April",
        "May": "May",
        "Jun": "June",
        "Jul": "July",
        "Aug": "August",
        "Sep": "September",
        "Oct": "October",
        "Nov": "November",
        "Dec": "December"
    }

    for key, value in find_replace.items():
        if key in months_year:
            return months_year.replace(key, value)
        
    return months_year


    