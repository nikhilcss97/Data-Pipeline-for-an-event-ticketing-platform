import pandas as pd

from src.data_generation.final_data_scripts.generate_xml import get_xml
from src.data_generation.universe import (
    COMMISSION_PATH, EVENT_PATH, NEW_DATE, ORGANIZER_PATH, RESELLER_PATH,
    TRANSACTION_PATH
)
from src.data_generation.utils import read_jsons, write_to_csv, write_to_xml

TRANSACTION_RAW_DATA_PATH = TRANSACTION_PATH + "/" + NEW_DATE
ORGANIZERS_RAW_DATA_PATH = ORGANIZER_PATH + "/" + NEW_DATE
RESELLERS_RAW_DATA_PATH = RESELLER_PATH + "/" + NEW_DATE
EVENTS_RAW_DATA_PATH = EVENT_PATH + "/" + NEW_DATE
COMMISSIONS_RAW_DATA_PATH = COMMISSION_PATH + "/" + NEW_DATE
FINAL_DATA_BASE_PATH = "/home/nikhil/toptal/final_data"


def populate_transaction_final_data():
    transactions = read_jsons(TRANSACTION_RAW_DATA_PATH)
    if not transactions:
        return

    df = pd.DataFrame(transactions)
    organizer_df = df[df["ticket_sold_by"] == 'organizer']
    reseller_df = df[df["ticket_sold_by"] == 'reseller']

    # Divide reseller_df into 3 parts
    reseller_csv_df = reseller_df[reseller_df["reseller_id"] % 3 == 0]
    reseller_xml_df = reseller_df[reseller_df["reseller_id"] % 3 == 1]
    reseller_db_df = reseller_df[reseller_df["reseller_id"] % 3 == 2]

    reseller_csv_df.drop(["ticket_sold_by"], axis=1, inplace=True)
    reseller_xml_df.drop(["ticket_sold_by"], axis=1, inplace=True)
    reseller_csv_df["reseller_id"] = reseller_csv_df["reseller_id"].astype("Int64")
    reseller_xml_df["reseller_id"] = reseller_xml_df["reseller_id"].astype("Int64")
    reseller_db_df["reseller_id"] = reseller_db_df["reseller_id"].astype("Int64")

    # Populates the 3 data sources
    populate_csv(reseller_csv_df, FINAL_DATA_BASE_PATH)
    populate_xml(reseller_xml_df, FINAL_DATA_BASE_PATH)
    final_df = reseller_db_df.append(organizer_df)
    final_df["reseller_id"] = final_df["reseller_id"].astype("Int64")
    populate_db(final_df, FINAL_DATA_BASE_PATH)


def populate_db_tables_data(folder, raw_data_path, final_data_base_path):
    raw_json_data = read_jsons(raw_data_path)
    if not raw_json_data:
        return
    df = pd.DataFrame(raw_json_data)

    unique_dates = df.created_date.unique()
    for date in unique_dates:
        data = df[df.created_date == date]
        path = f"{final_data_base_path}/db/{folder}/{date}/{folder}_{date}.csv"
        write_to_csv(data, path)


def populate_xml(df, base_path):
    if len(df) == 0:
        return
    unique_resellers = df.reseller_id.unique()
    unique_dates = df.transaction_date.unique()

    for reseller in unique_resellers:
        for date in unique_dates:
            data = df[(df.reseller_id == reseller) & (df.transaction_date == date)]
            path = f"{base_path}/xml/{date}/daily_sales_{date}_{reseller}.xml"
            xml_data = get_xml(data)
            write_to_xml(xml_data, path)


def populate_csv(df, base_path):
    if len(df) == 0:
        return
    unique_resellers = df.reseller_id.unique()
    unique_dates = df.transaction_date.unique()

    for reseller in unique_resellers:
        for date in unique_dates:
            data = df[(df.reseller_id == reseller) & (df.transaction_date == date)]
            path = f"{base_path}/csv/{date}/daily_sales_{date}_{reseller}.csv"
            write_to_csv(data, path)


def populate_db(df, base_path):
    if len(df) == 0:
        return
    unique_dates = df.transaction_date.unique()
    for date in unique_dates:
        data = df[df.transaction_date == date]
        path = f"{base_path}/db/transactions/{date}/daily_transactions_{date}.csv"
        write_to_csv(data, path)


populate_transaction_final_data()
populate_db_tables_data("organizers", ORGANIZERS_RAW_DATA_PATH, FINAL_DATA_BASE_PATH)
populate_db_tables_data("resellers", RESELLERS_RAW_DATA_PATH, FINAL_DATA_BASE_PATH)
populate_db_tables_data("events", EVENTS_RAW_DATA_PATH, FINAL_DATA_BASE_PATH)
populate_db_tables_data("commissions", COMMISSIONS_RAW_DATA_PATH, FINAL_DATA_BASE_PATH)
