"""LIST OF THINGS TO UPDATE BEFORE GENERATING DATA"""
# from src.data_generation.raw_data_scripts.main import (
#     COMMISSION_PATH, EVENT_PATH, ORGANIZER_PATH, RESELLER_PATH,
#     TRANSACTION_PATH
# )
from src.data_generation.utils import read_jsons

BASE_PATH = "/home/nikhil/toptal/raw_data/"
ORGANIZER_PATH = BASE_PATH + 'organizer/'
RESELLER_PATH = BASE_PATH + 'reseller/'
EVENT_PATH = BASE_PATH + 'event/'
COMMISSION_PATH = BASE_PATH + 'commission/'
TRANSACTION_PATH = BASE_PATH + 'transaction/'

NEW_DATE = "2022-10-02"
# Update IDs at the last
LAST_ORGANIZER_ID = 20
LAST_RESELLER_ID = 50
LAST_EVENT_ID = 100
LAST_TRANSACTION_ID = 2000

EVENT_TYPES = ["convention", "seminar", "sports", "festival", "spiritual"]
LOCATIONS = ["Mumbai Suburb", "Bangalore HSR", "Chennai", "South Bombay", "Delhi-NCR", "Gurgaon"]
SALES_CHANNELS = ["On-The-Site", "Web", "Mobile App", "Offline"]

ORGANIZERS = read_jsons(ORGANIZER_PATH)
RESELLERS = read_jsons(RESELLER_PATH)
EVENTS = read_jsons(EVENT_PATH)
COMMISSIONS = read_jsons(COMMISSION_PATH)
TRANSACTIONS = read_jsons(TRANSACTION_PATH)
