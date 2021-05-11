-- HIVE-24770
UPDATE "APP".SERDES SET SLIB='org.apache.hadoop.hive.serde2.MultiDelimitSerDe' where SLIB='org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe';

-- These lines need to be last.  Insert any changes above.
UPDATE "APP".CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.10.0-Update1', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.10.0' where VER_ID=1;
