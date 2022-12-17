COPY INTO "EVENT_TICKETING_DB"."STAGING"."TRANSACTION" FROM (
    SELECT t.$1,
           t.$2,
           t.$3,
           t.$4,
           t.$5,
           t.$6,
           t.$7,
           t.$8,
           t.$9,
           t.$10,
           t.$11,
           metadata$filename AS s3_source_filename
    FROM @EVENT_TICKETING_DB.STAGING.MY_DB_STAGE/transactions/ (
             FILE_FORMAT => "EVENT_TICKETING_DB"."STAGING".CSV_FORMAT_WITHOUT_HEADER
             ) t
) ON_ERROR = 'ABORT_STATEMENT' PURGE = FALSE;
SELECT COUNT(1)
FROM "EVENT_TICKETING_DB"."STAGING"."TRANSACTION";