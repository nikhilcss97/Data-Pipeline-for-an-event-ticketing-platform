import copy
import random

from src.data_generation.universe import LAST_ORGANIZER_ID, LOCATIONS, NEW_DATE


def generate_organizers(new_records=5):
    organizers_data = []
    blueprint = {
        "id": None,
        "name": None,
        "location": None,
        "created_date": NEW_DATE
    }

    for i in range(LAST_ORGANIZER_ID + 1, LAST_ORGANIZER_ID + 1 + new_records):
        blueprint["id"] = i
        blueprint["name"] = f"ORG_{i}"
        blueprint["location"] = random.choice(LOCATIONS)

        organizers_data.append(copy.deepcopy(blueprint))

    return organizers_data
