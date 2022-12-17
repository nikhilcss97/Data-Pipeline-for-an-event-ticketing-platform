BEGIN TRANSACTION;

MERGE INTO event_ticketing_db.raw.organizer raw
    USING
        (
            SELECT staging.id                                                       AS id,
                   staging.name                                                     AS name,
                   staging.location                                                 AS location,
                   staging.created_date                                             AS created_date,
                   staging.s3_source_file_name                                      AS s3_source_file_name,
                   convert_timezone('Asia/Kolkata', current_timestamp())::timestamp AS cdc_timestamp
            FROM event_ticketing_db.staging.organizer staging
                QUALIFY ROW_NUMBER() OVER (PARTITION BY id
                    ORDER BY created_date DESC ) = 1
        ) staging ON raw.id = staging.id
    WHEN MATCHED
        AND raw.created_date < staging.created_date THEN
        UPDATE
            SET
                raw.id = staging.id,
                raw.name = staging.name,
                raw.location = staging.location,
                raw.created_date = staging.created_date,
                raw.s3_source_file_name = staging.s3_source_file_name,
                raw.cdc_timestamp = staging.cdc_timestamp
    WHEN NOT MATCHED THEN
        INSERT
            (
             id,
             name,
             location,
             created_date,
             s3_source_file_name,
             cdc_timestamp
                )
            VALUES (staging.id,
                    staging.name,
                    staging.location,
                    staging.created_date,
                    staging.s3_source_file_name,
                    staging.cdc_timestamp);

DELETE
FROM event_ticketing_db.staging.organizer;

COMMIT;