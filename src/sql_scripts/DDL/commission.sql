CREATE OR REPLACE TABLE EVENT_TICKETING_DB.RAW.COMMISSION (
    organizer_id int,
    event_id int,
    reseller_id int,
    commission_rate float,
    created_date date,
    S3_SOURCE_FILE_NAME varchar(200),
    CDC_TIMESTAMP timestamp_ntz
);
CREATE OR REPLACE TABLE EVENT_TICKETING_DB.STAGING.COMMISSION (
    organizer_id int,
    event_id int,
    reseller_id int,
    commission_rate float,
    created_date date,
    S3_SOURCE_FILE_NAME varchar(200)
);