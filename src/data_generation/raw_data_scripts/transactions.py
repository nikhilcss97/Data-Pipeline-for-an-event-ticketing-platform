import copy
import random

from src.data_generation.universe import COMMISSIONS, EVENTS, LAST_TRANSACTION_ID, NEW_DATE, SALES_CHANNELS


def generate_transactions(new_records=1000):
    transactions_data = []
    blueprint = {
        "id": None,
        "customer_first_name": None,
        "customer_last_name": None,
        "ticket_sold_by": None,
        "organizer_id": None,
        "event_id": None,
        "reseller_id": None,
        "sales_channel": None,
        "number_of_purchased_tickets": None,
        "total_amount": None,
        "transaction_date": NEW_DATE
    }

    for i in range(LAST_TRANSACTION_ID + 1, LAST_TRANSACTION_ID + 1 + new_records):
        blueprint["id"] = i
        blueprint["customer_first_name"] = f"CFN_{i}"
        blueprint["customer_last_name"] = f"CLN_{i}"
        if i % 5 != 0:
            blueprint["ticket_sold_by"] = "reseller"
            commission_obj = random.choice(COMMISSIONS)
            blueprint["organizer_id"] = commission_obj["organizer_id"]
            blueprint["event_id"] = commission_obj["event_id"]
            blueprint["reseller_id"] = commission_obj["reseller_id"]
        else:
            blueprint["ticket_sold_by"] = "organizer"
            events_obj = random.choice(EVENTS)
            blueprint["organizer_id"] = events_obj["organizer_id"]
            blueprint["event_id"] = events_obj["id"]
            blueprint["reseller_id"] = None

        blueprint["sales_channel"] = random.choice(SALES_CHANNELS)
        blueprint["number_of_purchased_tickets"] = random.choice(range(1, 31))
        unit_price = random.choice([x * 1000 for x in range(1, 31)])
        blueprint["total_amount"] = blueprint["number_of_purchased_tickets"] * unit_price

        transactions_data.append(copy.deepcopy(blueprint))

    return transactions_data
