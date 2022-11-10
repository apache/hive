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

CREATE TABLE t_test1(
  id int,
  int_col int,
  year int,
  month int
);

CREATE TABLE t_test2(
  id int,
  int_col int,
  year int,
  month int
);

INSERT INTO t_test1 VALUES (1, 1, 2009, 1), (10,0, 2009, 1);
INSERT INTO t_test2 VALUES (1, 1, 2009, 1);

select id, int_col, year, month from t_test1 s where s.int_col = (select count(*) from t_test2 t where s.id = t.id) order by id;

