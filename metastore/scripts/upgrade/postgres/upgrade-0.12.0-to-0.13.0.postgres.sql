SELECT 'Upgrading MetaStore schema from 0.12.0 to 0.13.0';

CREATE FUNCTION hive13_to_date(date_str text) RETURNS DATE AS $$
DECLARE dt DATE;
BEGIN
  dt := date_str::DATE;
  RETURN dt;
EXCEPTION
  WHEN others THEN RETURN null;
END;
$$ LANGUAGE plpgsql;

UPDATE "PARTITION_KEY_VALS"
SET "PART_KEY_VAL" = COALESCE(TO_CHAR(hive13_to_date(src."PART_KEY_VAL"),'YYYY-MM-DD'), src."PART_KEY_VAL")
FROM "PARTITION_KEY_VALS" src
  INNER JOIN "PARTITIONS" ON src."PART_ID" = "PARTITIONS"."PART_ID"
  INNER JOIN "PARTITION_KEYS" ON "PARTITION_KEYS"."TBL_ID" = "PARTITIONS"."TBL_ID"
    AND "PARTITION_KEYS"."INTEGER_IDX" = src."INTEGER_IDX"
    AND "PARTITION_KEYS"."PKEY_TYPE" = 'date';

DROP FUNCTION hive13_to_date(date_str text);

UPDATE "VERSION" SET "SCHEMA_VERSION"='0.13.0', "VERSION_COMMENT"='Hive release version 0.13.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 0.12.0 to 0.13.0';


