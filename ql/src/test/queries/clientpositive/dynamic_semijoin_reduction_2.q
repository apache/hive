set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.dynamic.semijoin.reduction.threshold=-999999999999;

CREATE TABLE `table_1`(
  `bigint_col_7` bigint,
  `decimal2016_col_26` decimal(20,16),
  `tinyint_col_3` tinyint,
  `decimal2612_col_77` decimal(26,12),
  `timestamp_col_9` timestamp);

CREATE TABLE `table_18`(
  `tinyint_col_15` tinyint,
  `decimal2709_col_9` decimal(27,9),
  `tinyint_col_20` tinyint,
  `smallint_col_19` smallint,
  `decimal1911_col_16` decimal(19,11),
  `timestamp_col_18` timestamp);

-- HIVE-15904
EXPLAIN
SELECT
COUNT(*)
FROM table_1 t1
INNER JOIN table_18 t2 ON (((t2.tinyint_col_15) = (t1.bigint_col_7)) AND
((t2.decimal2709_col_9) = (t1.decimal2016_col_26))) AND
((t2.tinyint_col_20) = (t1.tinyint_col_3))
WHERE (t2.smallint_col_19) IN (SELECT
COALESCE(-92, -994) AS int_col
FROM table_1 tt1
INNER JOIN table_18 tt2 ON (tt2.decimal1911_col_16) = (tt1.decimal2612_col_77)
WHERE (t1.timestamp_col_9) = (tt2.timestamp_col_18));

drop table table_1;
drop table table_18;
