--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.adaptor.usage.mode=chosen;

CREATE TEMPORARY FUNCTION CDATE_SUB AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFCustomDateSub';

set hive.vectorized.adaptor.custom.udf.whitelist=org.apache.hadoop.hive.ql.udf.generic.GenericUDFCustomDateSub;

EXPLAIN VECTORIZATION
SELECT CDATE_SUB('2000-01-01', 1) FROM src
UNION ALL
SELECT CDATE_SUB('2000-01-01', CAST(key as int)) FROM src
UNION ALL
SELECT CDATE_SUB('2000-01-01', 1) FROM src WHERE key IS NOT NULL
UNION ALL
SELECT CDATE_SUB('2000-01-01', CAST(key as int)) FROM src WHERE key Is NOT NULL
UNION ALL
SELECT CDATE_SUB('2000-01-01', 1) FROM src WHERE key <> '' AND value <> ''
UNION ALL
SELECT CDATE_SUB('2000-01-01', CAST(key as int)) FROM src WHERE key <> '' AND value <> '';

