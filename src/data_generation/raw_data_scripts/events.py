import copy
import random

from src.data_generation.universe import EVENT_TYPES, LAST_EVENT_ID, NEW_DATE, ORGANIZERS


def generate_events(new_records=5):
    events_data = []
    blueprint = {
        "id": None,
        "name": None,
        "type": None,
        "organizer_id": None,
        "created_date": NEW_DATE
    }

    for i in range(LAST_EVENT_ID + 1, LAST_EVENT_ID + 1 + new_records):
        blueprint["id"] = i
        blueprint["name"] = f"EVENT_{i}"
        blueprint["type"] = random.choice(EVENT_TYPES)
        blueprint["organizer_id"] = random.choice(ORGANIZERS)["id"]

        events_data.append(copy.deepcopy(blueprint))

    return events_data
