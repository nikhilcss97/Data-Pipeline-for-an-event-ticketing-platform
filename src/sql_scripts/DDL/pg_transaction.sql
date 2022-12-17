CREATE TABLE TRANSACTION (
    id int NOT NULL,
    customer_first_name varchar(256) NULL,
    customer_last_name varchar(256) NULL,
    ticket_sold_by varchar(20) NOT NULL,
    organizer_id int NOT NULL,
    event_id int NOT NULL,
    reseller_id int NULL,
    sales_channel varchar(20) NULL,
    number_of_purchased_tickets int NOT NULL,
    total_amount float NOT NULL,
    transaction_date date NOT NULL,
    PRIMARY KEY (id)
);