from src.python_scripts.utils import generate_file_paths

DATES = []


def generate_reseller_copy_command(dates, base_path="/home/nikhil/toptal/final_data/db/resellers"):
    """Sample base_path = '/home/nikhil/toptal/final_data/db/resellers' """
    paths = generate_file_paths(base_path, dates)
    queries = []
    for path in paths:
        queries.append(f'''COPY reseller(id,name,location,created_date)
        FROM '{path}'
        DELIMITER ','
        CSV HEADER;''')
    return queries


def generate_event_copy_command(dates, base_path="/home/nikhil/toptal/final_data/db/events"):
    """Sample base_path = '/home/nikhil/toptal/final_data/db/events' """
    paths = generate_file_paths(base_path, dates)
    queries = []
    for path in paths:
        queries.append(f'''COPY event(id,event_name,event_type,organizer_id,created_date)
        FROM '{path}'
        DELIMITER ','
        CSV HEADER;''')
    return queries


def generate_commission_copy_command(dates, base_path="/home/nikhil/toptal/final_data/db/commissions"):
    """Sample base_path = '/home/nikhil/toptal/final_data/db/commissions' """
    paths = generate_file_paths(base_path, dates)
    queries = []
    for path in paths:
        queries.append(f'''COPY commission(organizer_id,event_id,reseller_id,commission_rate,created_date)
        FROM '{path}'
        DELIMITER ','
        CSV HEADER;''')
    return queries


def generate_transaction_copy_command(dates, base_path="/home/nikhil/toptal/final_data/db/transactions"):
    """Sample base_path = '/home/nikhil/toptal/final_data/db/transactions' """
    paths = generate_file_paths(base_path, dates)
    queries = []
    for path in paths:
        queries.append(f'''COPY transaction(id,customer_first_name,customer_last_name,ticket_sold_by,organizer_id,
        event_id, reseller_id,sales_channel,number_of_purchased_tickets,total_amount,transaction_date)
        FROM '{path}'
        DELIMITER ','
        CSV HEADER;''')
    return queries


def generate_organizer_copy_command(dates, base_path="/home/nikhil/toptal/final_data/db/organizers"):
    """Sample base_path = '/home/nikhil/toptal/final_data/db/organizers' """
    paths = generate_file_paths(base_path, dates)
    queries = []
    for path in paths:
        queries.append(f'''COPY organizer(id,name,location,created_date)
        FROM '{path}'
        DELIMITER ','
        CSV HEADER;''')
    return queries


def main(dates):
    copy_queries = []
    copy_queries += generate_organizer_copy_command(dates=dates)
    copy_queries += generate_transaction_copy_command(dates=dates)
    copy_queries += generate_commission_copy_command(dates=dates)
    copy_queries += generate_reseller_copy_command(dates=dates)
    copy_queries += generate_event_copy_command(dates=dates)

    return copy_queries
