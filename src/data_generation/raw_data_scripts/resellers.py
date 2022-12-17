import copy
import random

from src.data_generation.universe import LAST_RESELLER_ID, LOCATIONS, NEW_DATE


def generate_resellers(new_records=5):
    resellers_data = []
    blueprint = {
        "id": None,
        "name": None,
        "location": None,
        "created_date": NEW_DATE
    }

    for i in range(LAST_RESELLER_ID + 1, LAST_RESELLER_ID + 1 + new_records):
        blueprint["id"] = i
        blueprint["name"] = f"RESELLER_{i}"
        blueprint["location"] = random.choice(LOCATIONS)

        resellers_data.append(copy.deepcopy(blueprint))

    return resellers_data
