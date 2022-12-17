BEGIN TRANSACTION;
-- Write Logic to take care of duplicates in transactions_combined table
CREATE OR REPLACE TEMPORARY TABLE transactions_combined AS (
    SELECT staging.transaction_id                                           AS transaction_id,
           staging.customer_first_name                                      AS customer_first_name,
           staging.customer_last_name                                       AS customer_last_name,
           'reseller'                                                       AS ticket_sold_by,
           staging.organizer_id                                             AS organizer_id,
           staging.event_id                                                 AS event_id,
           staging.reseller_id                                              AS reseller_id,
           staging.sales_channel                                            AS sales_channel,
           staging.number_of_purchased_tickets                              AS number_of_purchased_tickets,
           staging.total_amount                                             AS total_amount,
           staging.transaction_date                                         AS transaction_date,
           staging.s3_source_file_name                                      AS s3_source_file_name,
           convert_timezone('Asia/Kolkata', current_timestamp())::timestamp AS cdc_timestamp
    FROM event_ticketing_db.staging.csv_data staging
        QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id
            ORDER BY transaction_date DESC ) = 1
    UNION ALl
    SELECT XMLGET(staging.record_content, 'transactionId'):"$"::int            AS transaction_id,
           XMLGET(XMLGET(record_content, 'customer'), 'firstName'):"$"::string AS customer_first_name,
           XMLGET(XMLGET(record_content, 'customer'), 'lastName'):"$"::string  AS customer_last_name,
           'reseller'                                                          AS ticket_sold_by,
           XMLGET(staging.record_content, 'organizerId'):"$"::int              AS organizer_id,
           XMLGET(staging.record_content, 'eventId'):"$"::int                  AS event_id,
           XMLGET(staging.record_content, 'resellerId'):"$"::int               AS reseller_id,
           XMLGET(staging.record_content, 'salesChannel'):"$"::string          AS sales_channel,
           XMLGET(staging.record_content, 'numberOfPurchasedTickets'):"$"::int AS number_of_purchased_tickets,
           XMLGET(staging.record_content, 'totalAmount'):"$"::float            AS total_amount,
           XMLGET(staging.record_content, 'transactionDate'):"$"::date         AS transaction_date,
           staging.s3_source_file_name                                         AS s3_source_file_name,
           convert_timezone('Asia/Kolkata', current_timestamp())::timestamp    AS cdc_timestamp
    FROM event_ticketing_db.staging.xml_data staging
        QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id
            ORDER BY transaction_date DESC ) = 1
    UNION ALL
    SELECT staging.id                                                       AS id,
           staging.customer_first_name                                      AS customer_first_name,
           staging.customer_last_name                                       AS customer_last_name,
           staging.ticket_sold_by                                           AS ticket_sold_by,
           staging.organizer_id                                             AS organizer_id,
           staging.event_id                                                 AS event_id,
           staging.reseller_id                                              AS reseller_id,
           staging.sales_channel                                            AS sales_channel,
           staging.number_of_purchased_tickets                              AS number_of_purchased_tickets,
           staging.total_amount                                             AS total_amount,
           staging.transaction_date                                         AS transaction_date,
           staging.s3_source_file_name                                      AS s3_source_file_name,
           convert_timezone('Asia/Kolkata', current_timestamp())::timestamp AS cdc_timestamp
    FROM event_ticketing_db.staging.transaction staging
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id
            ORDER BY transaction_date DESC ) = 1
);

CREATE OR REPLACE TEMPORARY TABLE master_sales AS (
    SELECT tc.transaction_id                                                         AS transaction_id,
           e.event_name                                                              AS event_name,
           e.event_type                                                              AS event_type,
           tc.number_of_purchased_tickets                                            AS number_of_purchased_tickets,
           tc.total_amount                                                           AS total_amount,
           tc.transaction_date                                                       AS transaction_date,
           tc.ticket_sold_by                                                         AS ticket_sold_by,
           tc.organizer_id                                                           AS organizer_id,
           tc.reseller_id                                                            AS reseller_id,
           o.name                                                                    AS organizer_name,
           o.location                                                                AS organizer_location,
           r.name                                                                    AS reseller_name,
           r.location                                                                AS reseller_location,
           c.commission_rate                                                         AS commission_rate,
           COALESCE(COALESCE(c.commission_rate, 0) * tc.total_amount * 1.0, 0) / 100 AS total_commission,
           tc.customer_first_name                                                    AS customer_first_name,
           tc.customer_last_name                                                     AS customer_last_name,
           tc.sales_channel                                                          AS sales_channel,
           tc.s3_source_file_name                                                    AS s3_source_file_name,
           convert_timezone('Asia/Kolkata', current_timestamp())::timestamp          AS cdc_timestamp
    FROM transactions_combined tc
             LEFT JOIN
         event_ticketing_db.raw.event e
         ON
             tc.event_id = e.id
             LEFT JOIN
         event_ticketing_db.raw.organizer o
         ON
             tc.organizer_id = o.id
             LEFT JOIN
         event_ticketing_db.raw.reseller r
         ON
             tc.reseller_id = r.id
             LEFT JOIN
         event_ticketing_db.raw.commission c
         ON
                 tc.organizer_id = c.organizer_id AND tc.event_id = c.event_id AND tc.reseller_id = c.reseller_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id
            ORDER BY transaction_date DESC ) = 1
);


MERGE INTO event_ticketing_db.analytical.sales analytical
    USING master_sales staging
    ON analytical.transaction_id = staging.transaction_id
    WHEN MATCHED
        AND analytical.transaction_date < staging.transaction_date THEN
        UPDATE
            SET
                analytical.transaction_id = staging.transaction_id,
                analytical.event_name = staging.event_name,
                analytical.event_type = staging.event_type,
                analytical.number_of_purchased_tickets = staging.number_of_purchased_tickets,
                analytical.total_amount = staging.total_amount,
                analytical.transaction_date = staging.transaction_date,
                analytical.ticket_sold_by = staging.ticket_sold_by,
                analytical.organizer_id = staging.organizer_id,
                analytical.reseller_id = staging.reseller_id,
                analytical.organizer_name = staging.organizer_name,
                analytical.organizer_location = staging.organizer_location,
                analytical.reseller_name = staging.reseller_name,
                analytical.reseller_location = staging.reseller_location,
                analytical.commission_rate = staging.commission_rate,
                analytical.total_commission = staging.total_commission,
                analytical.customer_first_name = staging.customer_first_name,
                analytical.customer_last_name = staging.customer_last_name,
                analytical.sales_channel = staging.sales_channel,
                analytical.s3_source_file_name = staging.s3_source_file_name,
                analytical.cdc_timestamp = staging.cdc_timestamp
    WHEN NOT MATCHED THEN
        INSERT
            (
             transaction_id,
             event_name,
             event_type,
             number_of_purchased_tickets,
             total_amount,
             transaction_date,
             ticket_sold_by,
             organizer_id,
             reseller_id,
             organizer_name,
             organizer_location,
             reseller_name,
             reseller_location,
             commission_rate,
             total_commission,
             customer_first_name,
             customer_last_name,
             sales_channel,
             s3_source_file_name,
             cdc_timestamp
                )
            VALUES (staging.transaction_id,
                    staging.event_name,
                    staging.event_type,
                    staging.number_of_purchased_tickets,
                    staging.total_amount,
                    staging.transaction_date,
                    staging.ticket_sold_by,
                    staging.organizer_id,
                    staging.reseller_id,
                    staging.organizer_name,
                    staging.organizer_location,
                    staging.reseller_name,
                    staging.reseller_location,
                    staging.commission_rate,
                    staging.total_commission,
                    staging.customer_first_name,
                    staging.customer_last_name,
                    staging.sales_channel,
                    staging.s3_source_file_name,
                    staging.cdc_timestamp);

DELETE
FROM event_ticketing_db.staging.xml_data;

DELETE
FROM event_ticketing_db.staging.csv_data;

DELETE
FROM event_ticketing_db.staging.transaction;

DROP TABLE master_sales;
DROP TABLE transactions_combined;

COMMIT;