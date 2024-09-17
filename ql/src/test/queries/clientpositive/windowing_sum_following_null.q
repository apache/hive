
create table sum_window_test_small (id int, tinyint_col tinyint, double_col double, decimal_col decimal(12,2));
insert into sum_window_test_small values (3, 17, 17.1, 17.1), (4, 14, 14.1, 14.1), (6, 18, 18.1, 18.1),
    (7, 19, 19.1, 19.1), (8,NULL, NULL, NULL), (10, NULL, NULL, NULL), (11, 22, 22.0, 22.1);
select id,
tinyint_col,
sum(tinyint_col) over (order by id nulls last rows between 1 following and 2 following),
sum(double_col) over (order by id nulls last rows between 1 following and 2 following),
sum(decimal_col) over (order by id nulls last rows between 1 following and 2 following)
from sum_window_test_small order by id;

-- check if it works with a null at the end
insert into sum_window_test_small values (12,NULL, NULL, NULL);

select id,
tinyint_col,
sum(tinyint_col) over (order by id nulls last rows between 1 following and 2 following),
sum(double_col) over (order by id nulls last rows between 1 following and 2 following),
sum(decimal_col) over (order by id nulls last rows between 1 following and 2 following)
from sum_window_test_small order by id;

-- check if it works with two nulls at the end
insert into sum_window_test_small values (13,NULL, NULL, NULL);

select id,
tinyint_col,
sum(tinyint_col) over (order by id nulls last rows between 1 following and 2 following),
sum(double_col) over (order by id nulls last rows between 1 following and 2 following),
sum(decimal_col) over (order by id nulls last rows between 1 following and 2 following)
from sum_window_test_small order by id;
