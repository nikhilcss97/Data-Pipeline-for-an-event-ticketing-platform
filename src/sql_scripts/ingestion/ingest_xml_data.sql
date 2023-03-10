COPY INTO "EVENT_TICKETING_DB"."STAGING"."XML_DATA" FROM (
    SELECT t.$1, metadata$filename as s3_source
    FROM @EVENT_TICKETING_DB.STAGING.MY_XML_STAGE/
             (FILE_FORMAT => "EVENT_TICKETING_DB"."STAGING".XML_FILE_FORMAT) t
) ON_ERROR = 'ABORT_STATEMENT' PURGE = FALSE;
SELECT COUNT(1)
FROM "EVENT_TICKETING_DB"."STAGING"."XML_DATA";