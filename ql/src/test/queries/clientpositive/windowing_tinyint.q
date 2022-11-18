CREATE TABLE t_test(
  int_col int,
  tinyint_col tinyint
);

insert into t_test values (1, 1);

select 
count(int_col) over (order by tinyint_col)
from t_test;
