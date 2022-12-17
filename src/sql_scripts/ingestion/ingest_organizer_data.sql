COPY INTO "EVENT_TICKETING_DB"."STAGING"."ORGANIZER" FROM (
    SELECT t.$1, t.$2, t.$3, t.$4, metadata$filename AS s3_source_filename
    FROM @EVENT_TICKETING_DB.STAGING.MY_DB_STAGE/organizers/ (
             FILE_FORMAT => "EVENT_TICKETING_DB"."STAGING".CSV_FORMAT_WITHOUT_HEADER
             ) t
) ON_ERROR = 'ABORT_STATEMENT' PURGE = FALSE;
SELECT COUNT(1)
FROM "EVENT_TICKETING_DB"."STAGING"."ORGANIZER";