--! qt:dataset:src

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
