-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table if not exists source_table_001 (
MY_DATE date,
MY_ID bigint,
MY_ID2 bigint,
ENVIRONMENT string,
DOWN_VOLUME bigint,
UP_VOLUME bigint
)
stored AS ORC
TBLPROPERTIES("transactional"="true");
insert into table source_table_001
  values ('2010-10-10', 1, 1, 'env', 1, 1);
analyze table source_table_001 compute statistics for columns;

CREATE MATERIALIZED VIEW source_table_001_mv AS
SELECT
SUM(A.DOWN_VOLUME) AS DOWN_VOLUME_SUM,
SUM(A.UP_VOLUME) AS UP_VOLUME_SUM,
A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
from source_table_001 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,A.MY_DATE;
analyze table source_table_001_mv compute statistics for columns;


explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
FROM source_table_001 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,A.MY_DATE;

explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
FROM source_table_001 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,A.MY_DATE
LIMIT 100;

explain
select
1,
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
FROM source_table_001 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,A.MY_DATE;

explain
select
SUM(A.DOWN_VOLUME) + 0 AS DOWNLOAD_VOLUME_BYTES,
A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
FROM source_table_001 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,A.MY_DATE;

-- DOES NOT WORK - PROBLEM IN FIELD TRIMMER WITH OBY
explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
FROM source_table_001 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,A.MY_DATE
ORDER BY A.MY_ID2 
LIMIT 100;

-- WORKS WITH COLUMN STATS, CBO FAILS WITHOUT
explain
select
distinct A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
FROM source_table_001 AS A;

explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
FROM source_table_001 AS A
where A.MY_DATE=TO_DATE('2010-01-10')
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,A.MY_DATE;

explain
select
SUM(A.DOWN_VOLUME) + SUM(A.UP_VOLUME) AS TOTAL_VOLUME_BYTES,
A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
FROM source_table_001 AS A
where A.MY_DATE=TO_DATE('2010-01-10')
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,A.MY_DATE;

explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES
FROM source_table_001 AS A
where A.MY_DATE=TO_DATE('2010-01-10');

explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
TO_DATE('2010-01-10')
FROM source_table_001 AS A
where A.MY_DATE=TO_DATE('2010-01-10');

explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
A.MY_DATE
FROM source_table_001 AS A
where A.MY_DATE=TO_DATE('2010-01-10')
group by A.MY_DATE;

drop materialized view source_table_001_mv;
