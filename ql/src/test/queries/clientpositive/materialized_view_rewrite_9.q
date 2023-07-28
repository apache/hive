-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table if not exists source_table_001_n0 (
MY_DATE timestamp,
MY_ID bigint,
MY_ID2 bigint,
ENVIRONMENT string,
DOWN_VOLUME bigint,
UP_VOLUME bigint
)
stored AS ORC
TBLPROPERTIES("transactional"="true");
insert into table source_table_001_n0
  values ('2010-10-10 00:00:00', 1, 1, 'env', 1, 1);
analyze table source_table_001_n0 compute statistics for columns;

CREATE MATERIALIZED VIEW source_table_001_mv_n0 AS
SELECT
SUM(A.DOWN_VOLUME) AS DOWN_VOLUME_SUM,
SUM(A.UP_VOLUME) AS UP_VOLUME_SUM,
A.MY_ID,A.MY_DATE,A.MY_ID2,A.ENVIRONMENT
from source_table_001_n0 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,A.MY_DATE;
analyze table source_table_001_mv_n0 compute statistics for columns;

explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
FLOOR(A.MY_DATE to hour),A.MY_ID2,A.ENVIRONMENT
FROM source_table_001_n0 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,FLOOR(A.MY_DATE to hour);

DROP MATERIALIZED VIEW source_table_001_mv_n0;

CREATE MATERIALIZED VIEW source_table_001_mv_n0 AS
SELECT
SUM(A.DOWN_VOLUME) AS DOWN_VOLUME_SUM,
SUM(A.UP_VOLUME) AS UP_VOLUME_SUM,
A.MY_ID,FLOOR(A.MY_DATE to hour),A.MY_ID2,A.ENVIRONMENT
from source_table_001_n0 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,FLOOR(A.MY_DATE to hour);
analyze table source_table_001_mv_n0 compute statistics for columns;

explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
FLOOR(A.MY_DATE to day),A.MY_ID2,A.ENVIRONMENT
FROM source_table_001_n0 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,FLOOR(A.MY_DATE to day);

explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
FLOOR(A.MY_DATE to hour),A.MY_ID2,A.ENVIRONMENT
FROM source_table_001_n0 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,FLOOR(A.MY_DATE to hour);

explain
select
SUM(A.DOWN_VOLUME) AS DOWNLOAD_VOLUME_BYTES,
FLOOR(A.MY_DATE to second),A.MY_ID2,A.ENVIRONMENT
FROM source_table_001_n0 AS A
group by A.MY_ID,A.MY_ID2,A.ENVIRONMENT,FLOOR(A.MY_DATE to second);

DROP MATERIALIZED VIEW source_table_001_mv_n0;
