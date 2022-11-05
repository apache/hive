--! qt:dataset:src
SET hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=false;
SET hive.optimize.topnkey=true;

SET hive.optimize.ppd=true;
SET hive.ppd.remove.duplicatefilters=true;
SET hive.tez.dynamic.partition.pruning=true;
SET hive.optimize.metadataonly=false;
SET hive.optimize.index.filter=true;
SET hive.tez.min.bloom.filter.entries=1;

SET hive.stats.fetch.column.stats=true;
SET hive.cbo.enable=true;

SET hive.optimize.topnkey=true;

CREATE TABLE t_test(
  a tinyint
);

INSERT INTO t_test VALUES
(0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

select 1 from (select count(distinct a) from t_test) x;
select b from (select count(distinct a) b from t_test) x;

explain cbo select 1 from (select count(distinct a) from t_test) x;
explain cbo select b from (select count(distinct a) b from t_test) x;

explain select 1 from (select count(distinct a) from t_test) x;
explain select b from (select count(distinct a) b from t_test) x;
