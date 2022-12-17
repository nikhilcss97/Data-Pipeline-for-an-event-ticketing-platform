import copy
import random

from src.data_generation.universe import EVENTS, LAST_EVENT_ID, LAST_ORGANIZER_ID, LAST_RESELLER_ID, NEW_DATE, RESELLERS


def generate_commissions(events_considered=10, resellers_per_event=3):
    commissions_data = []
    blueprint = {
        "organizer_id": None,
        "event_id": None,
        "reseller_id": None,
        "commission_rate": None,
        "created_date": NEW_DATE
    }

    for event_obj in random.sample(EVENTS, events_considered):
        resellers = []
        try:
            resellers = random.sample(RESELLERS, min(len(RESELLERS), resellers_per_event))
        except ValueError as e:
            print("Not enough resellers in the data. Kindly add more")

        for reseller in resellers:
            if reseller["id"] > LAST_RESELLER_ID or \
                    event_obj["organizer_id"] > LAST_ORGANIZER_ID or event_obj["id"] > LAST_EVENT_ID:
                blueprint["organizer_id"] = event_obj["organizer_id"]
                blueprint["event_id"] = event_obj["id"]
                blueprint["reseller_id"] = reseller["id"]
                blueprint["commission_rate"] = random.choice(range(5, 16))
                commissions_data.append(copy.deepcopy(blueprint))

    return commissions_data
