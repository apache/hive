create table t1 (a int, b string, c int);
insert into t1 (a, b, c) values
    (1, 'A', 1),
    (2, 'A', 2),
    (3, 'B', 1),
    (4, 'B', 2);

explain cbo
select a, b, c
  from t1
qualify row_number() over (partition by b order by c) = 1;

select a, b, c
  from t1
qualify row_number() over (partition by b order by c) = 1;

explain cbo
select a, b, c, row_number() over (partition by b order by c) as row_num
  from t1
qualify row_num = 1;

select a, b, c, row_number() over (partition by b order by c) as row_num
  from t1
qualify row_num = 1;

--explain cbo
--select a, b, c, row_number() over (partition by b order by c) as row_num
--  from t1
--qualify row_num = 1 and row_number() over (order by c) = 1;
--
--select a, b, c, row_number() over (partition by b order by c) as row_num
--  from t1
--qualify row_num = 1 and row_number() over (order by c) = 1;

--select c2, sum(c3) over (partition by c2) as r
--  from t1
--  where c3 < 4
--  group by c2, c3
--  having sum(c1) > 3
--  qualify r in (
--    select min(c1)
--      from test
--      group by c2
--      having min(c1) > 3);
