CREATE OR REPLACE TABLE EVENT_TICKETING_DB.RAW.EVENT (
    id int,
    event_name varchar,
    event_type varchar(50),
    organizer_id int,
    created_date date,
    S3_SOURCE_FILE_NAME varchar(200),
    CDC_TIMESTAMP timestamp_ntz
);
CREATE OR REPLACE TRANSIENT TABLE EVENT_TICKETING_DB.STAGING.EVENT (
    id int,
    event_name varchar,
    event_type varchar(50),
    organizer_id int,
    created_date date,
    S3_SOURCE_FILE_NAME varchar(200)
);