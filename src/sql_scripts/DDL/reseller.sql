CREATE OR REPLACE TABLE EVENT_TICKETING_DB.RAW.RESELLER (
    id int,
    name varchar(256),
    location varchar,
    created_date date,
    S3_SOURCE_FILE_NAME varchar(200),
    CDC_TIMESTAMP timestamp_ntz
);
CREATE OR REPLACE TRANSIENT TABLE EVENT_TICKETING_DB.STAGING.RESELLER (
    id int,
    name varchar(256),
    location varchar,
    created_date date,
    S3_SOURCE_FILE_NAME varchar(200)
);