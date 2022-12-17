CREATE OR REPLACE TABLE EVENT_TICKETING_DB.RAW.CSV_DATA (
    transaction_id int,
    customer_first_name varchar(256),
    customer_last_name varchar(256),
    organizer_id int,
    event_id int,
    reseller_id int,
    sales_channel varchar(20),
    number_of_purchased_tickets int,
    total_amount float,
    transaction_date date,
    S3_SOURCE_FILE_NAME varchar(200),
    CDC_TIMESTAMP timestamp_ntz
);
CREATE OR REPLACE TRANSIENT TABLE EVENT_TICKETING_DB.STAGING.CSV_DATA (
    transaction_id int,
    customer_first_name varchar(256),
    customer_last_name varchar(256),
    organizer_id int,
    event_id int,
    reseller_id int,
    sales_channel varchar(20),
    number_of_purchased_tickets int,
    total_amount float,
    transaction_date date,
    S3_SOURCE_FILE_NAME varchar(200)
);