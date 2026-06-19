CREATE TABLE grpby_test (int_col_5 INT,
  int_col_7 INT);

SET hive.mapred.mode=strict;

EXPLAIN
SELECT
int_col_7,
MAX(LEAST(COALESCE(int_col_5, -279),
  COALESCE(int_col_7, 476))) AS int_col
FROM grpby_test
GROUP BY
int_col_7,
int_col_7,
LEAST(COALESCE(int_col_5, -279),
  COALESCE(int_col_7, 476));

create table aGBY (i int, j string);
insert into aGBY values ( 1, 'a'),(2,'b');
explain cbo select min(j) from aGBY where j='a' group by j;
select min(j) from aGBY where j='a' group by j;
drop table aGBY;
