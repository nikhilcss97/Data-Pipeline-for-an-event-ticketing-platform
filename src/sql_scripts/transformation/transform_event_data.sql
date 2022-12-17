BEGIN TRANSACTION;

MERGE INTO event_ticketing_db.raw.event raw
    USING
        (
            SELECT staging.id                                                       AS id,
                   staging.event_name                                               AS event_name,
                   staging.event_type                                               AS event_type,
                   staging.organizer_id                                             AS organizer_id,
                   staging.created_date                                             AS created_date,
                   staging.s3_source_file_name                                      AS s3_source_file_name,
                   convert_timezone('Asia/Kolkata', current_timestamp())::timestamp AS cdc_timestamp
            FROM event_ticketing_db.staging.event staging
                QUALIFY ROW_NUMBER() OVER (PARTITION BY id
                    ORDER BY created_date DESC ) = 1
        ) staging ON raw.id = staging.id
    WHEN MATCHED
        AND raw.created_date < staging.created_date THEN
        UPDATE
            SET
                raw.id = staging.id,
                raw.event_name = staging.event_name,
                raw.event_type = staging.event_type,
                raw.organizer_id = staging.organizer_id,
                raw.created_date = staging.created_date,
                raw.s3_source_file_name = staging.s3_source_file_name,
                raw.cdc_timestamp = staging.cdc_timestamp
    WHEN NOT MATCHED THEN
        INSERT
            (
             id,
             event_name,
             event_type,
             organizer_id,
             created_date,
             s3_source_file_name,
             cdc_timestamp
                )
            VALUES (staging.id,
                    staging.event_name,
                    staging.event_type,
                    staging.organizer_id,
                    staging.created_date,
                    staging.s3_source_file_name,
                    staging.cdc_timestamp);

DELETE
FROM event_ticketing_db.staging.event;

COMMIT;