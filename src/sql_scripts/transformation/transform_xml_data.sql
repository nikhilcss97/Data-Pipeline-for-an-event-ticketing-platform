BEGIN TRANSACTION;

MERGE INTO event_ticketing_db.raw.xml_data raw
    USING
        (
            SELECT XMLGET(staging.record_content, 'transactionId'):"$"::int            AS transaction_id,
                   XMLGET(XMLGET(record_content, 'customer'), 'firstName'):"$"::string AS customer_first_name,
                   XMLGET(XMLGET(record_content, 'customer'), 'lastName'):"$"::string  AS customer_last_name,
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
        ) staging ON raw.transaction_id = staging.transaction_id
    WHEN MATCHED
        AND raw.transaction_date < staging.transaction_date THEN
        UPDATE
            SET
                raw.transaction_id = staging.transaction_id,
                raw.customer_first_name = staging.customer_first_name,
                raw.customer_last_name = staging.customer_last_name,
                raw.organizer_id = staging.organizer_id,
                raw.event_id = staging.event_id,
                raw.reseller_id = staging.reseller_id,
                raw.sales_channel = staging.sales_channel,
                raw.number_of_purchased_tickets = staging.number_of_purchased_tickets,
                raw.total_amount = staging.total_amount,
                raw.transaction_date = staging.transaction_date,
                raw.s3_source_file_name = staging.s3_source_file_name,
                raw.cdc_timestamp = staging.cdc_timestamp
    WHEN NOT MATCHED THEN
        INSERT
            (
             transaction_id,
             customer_first_name,
             customer_last_name,
             organizer_id,
             event_id,
             reseller_id,
             sales_channel,
             number_of_purchased_tickets,
             total_amount,
             transaction_date,
             s3_source_file_name,
             cdc_timestamp
                )
            VALUES (staging.transaction_id,
                    staging.customer_first_name,
                    staging.customer_last_name,
                    staging.organizer_id,
                    staging.event_id,
                    staging.reseller_id,
                    staging.sales_channel,
                    staging.number_of_purchased_tickets,
                    staging.total_amount,
                    staging.transaction_date,
                    staging.s3_source_file_name,
                    staging.cdc_timestamp);

COMMIT;