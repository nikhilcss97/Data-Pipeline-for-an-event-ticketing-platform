"""
Process to generate Data:
    Step 0: Update the DATE
    Step 1: Add new Organizers
    Step 2: Add new Resellers
    Step 3: Add new Events (Not necessary that all the newly added organizers will end up getting used here)
    Step 4: Add new Commissions if needed (Not necessary that we get mentioned number of new records)
    Step 5: Add new transactions
    Step 6: Update the Last IDs of Organizer, Reseller, Events, Transactions


    **NOTE: If adding a new data point manually in the DB, update the Last IDs in the universe
"""

from src.data_generation.raw_data_scripts.commission import generate_commissions
from src.data_generation.raw_data_scripts.events import generate_events
from src.data_generation.raw_data_scripts.organizers import generate_organizers
from src.data_generation.raw_data_scripts.resellers import generate_resellers
from src.data_generation.raw_data_scripts.transactions import generate_transactions
from src.data_generation.utils import write_to_file
from src.data_generation.universe import (
    COMMISSION_PATH, EVENT_PATH, NEW_DATE, ORGANIZER_PATH, RESELLER_PATH,
    TRANSACTION_PATH
)

"""RUN BLOCK BY BLOCK"""
# ------------------------------------
# organizers = generate_organizers(20)
# resellers = generate_resellers(50)
# write_to_file(organizers, f"{ORGANIZER_PATH}{NEW_DATE}/{NEW_DATE}_organizer_data.json")
# write_to_file(resellers, f"{RESELLER_PATH}{NEW_DATE}/{NEW_DATE}_reseller_data.json")
# ------------------------------------
# events = generate_events(100)
# write_to_file(events, f"{EVENT_PATH}{NEW_DATE}/{NEW_DATE}_event_data.json")
# # ------------------------------------
# commission = generate_commissions(events_considered=100, resellers_per_event=3)
# write_to_file(commission, f"{COMMISSION_PATH}{NEW_DATE}/{NEW_DATE}_commission_data.json")
# # ------------------------------------
# transactions = generate_transactions(2000)
# write_to_file(transactions, f"{TRANSACTION_PATH}{NEW_DATE}/{NEW_DATE}_transaction_data.json")
# ------------------------------------
