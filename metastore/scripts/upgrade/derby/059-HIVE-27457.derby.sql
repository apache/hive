-- update input/output format in SDS
UPDATE "SDS"
    SET "SDS"."INPUT_FORMAT" = "org.apache.hadoop.hive.kudu.KuduInputFormat",
        "SDS"."OUTPUT_FORMAT" = "org.apache.hadoop.hive.kudu.KuduOutputFormat"
    WHERE "SDS"."SD_ID" IN (
        SELECT "TBL_ID" FROM "TABLE_PARAMS" WHERE "PARAM_VALUE" LIKE '%KuduStorageHandler%'
    );

-- update serdes
UPDATE "SERDES"
    SET "SERDES"."SLIB" = "org.apache.hadoop.hive.kudu.KuduSerDe"
    WHERE "SERDE_ID" IN (
        SELECT "SDS.SERDE_ID"
            FROM "TBLS"
            LEFT JOIN "SDS" ON "TBLS"."SD_ID" = "SDS"."SD_ID"
            WHERE "TBL_ID" IN (SELECT "TBL_ID" FROM "TABLE_PARAMS" WHERE "PARAM_VALUE" LIKE '%KuduStorageHandler%')
    );