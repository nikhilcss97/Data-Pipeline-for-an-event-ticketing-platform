BEGIN TRANSACTION;

MERGE INTO event_ticketing_db.raw.commission raw
    USING
        (
            SELECT staging.organizer_id                                             AS organizer_id,
                   staging.event_id                                                 AS event_id,
                   staging.reseller_id                                              AS reseller_id,
                   staging.commission_rate                                          AS commission_rate,
                   staging.created_date                                             AS created_date,
                   staging.s3_source_file_name                                      AS s3_source_file_name,
                   convert_timezone('Asia/Kolkata', current_timestamp())::timestamp AS cdc_timestamp
            FROM event_ticketing_db.staging.commission staging
                QUALIFY ROW_NUMBER() OVER (PARTITION BY organizer_id, event_id, reseller_id
                    ORDER BY created_date DESC ) = 1
        ) staging ON raw.organizer_id = staging.organizer_id
    WHEN MATCHED
        AND raw.created_date < staging.created_date THEN
        UPDATE
            SET
                raw.organizer_id = staging.organizer_id,
                raw.event_id = staging.event_id,
                raw.reseller_id = staging.reseller_id,
                raw.commission_rate = staging.commission_rate,
                raw.created_date = staging.created_date,
                raw.s3_source_file_name = staging.s3_source_file_name,
                raw.cdc_timestamp = staging.cdc_timestamp
    WHEN NOT MATCHED THEN
        INSERT
            (
             organizer_id,
             event_id,
             reseller_id,
             commission_rate,
             created_date,
             s3_source_file_name,
             cdc_timestamp
                )
            VALUES (staging.organizer_id,
                    staging.event_id,
                    staging.reseller_id,
                    staging.commission_rate,
                    staging.created_date,
                    staging.s3_source_file_name,
                    staging.cdc_timestamp);

DELETE
FROM event_ticketing_db.staging.commission;

COMMIT;