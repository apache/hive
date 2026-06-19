set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

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

CREATE TABLE t_test3(
  id int,
  int_col int,
  year int,
  month int
);

CREATE TABLE t_test4(
  id int,
  int_col int,
  year int,
  month int
);


CREATE TABLE dummy (
  id int
) stored as orc TBLPROPERTIES ('transactional'='true');

CREATE MATERIALIZED VIEW need_a_mat_view_in_registry AS
SELECT * FROM dummy where id > 5;

INSERT INTO t_test1 VALUES (1, 1, 2009, 1), (10,0, 2009, 1);
INSERT INTO t_test2 VALUES (1, 1, 2009, 1);
INSERT INTO t_test3 VALUES (1, 1, 2009, 1);
INSERT INTO t_test4 VALUES (1, 1, 2009, 1);

select id, int_col, year, month from t_test1 s where s.int_col = (select count(*) from t_test2 t where s.id = t.id) order by id;
explain cbo select id, int_col, year, month from t_test1 s where s.int_col = (select count(*) from t_test2 t where s.id = t.id) order by id;

explain cbo
select id, int_col, year, month from t_test2 s where not (
  s.int_col in (select count(*) from t_test3 t2 where s.id = t2.id) and
  s.int_col in (select count(*) from t_test4 t3 where s.id = t3.id)
);
select id, int_col, year, month from t_test2 s where not (
  s.int_col in (select count(*) from t_test3 t2 where s.id = t2.id) and
  s.int_col in (select count(*) from t_test4 t3 where s.id = t3.id)
);

