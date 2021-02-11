create table my_test_table (
  id integer,
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  year int,
  month int,
  day int);

-- CDPD-20749
explain cbo physical
select count(distinct id)
from my_test_table t1
where t1.day not in
  (select tt1.tinyint_col as tinyint_col_1
   from my_test_table tt1
   where t1.smallint_col = tt1.smallint_col);

-- CDPD-20756
explain cbo physical
select count(t1.c) over () from
  (select max(int_col) c from my_test_table) t1
where t1.c not in
  (select sum(t1.smallint_col) from my_test_table t1);

-- CDPD-20759
explain cbo physical
WITH foo AS (SELECT 1 FROM my_test_table WHERE int_col IN (SELECT 1))
SELECT * FROM foo
UNION SELECT * FROM foo;

explain cbo physical
WITH foo AS (SELECT 1 FROM my_test_table WHERE int_col IN (SELECT 1))
SELECT * FROM (SELECT * FROM foo UNION SELECT * FROM foo) bar;

-- CDPD-20842
explain cbo physical
select t1.int_col
from my_test_table t1 left join
  (select coalesce(t1.year, 384) as int_col
   from my_test_table t1
   where t1.bigint_col in
     (select day as int_col from my_test_table where t1.id = day)) t2
on t2.int_col = t1.month
where t1.month is not null;
