create table t1 (a int, b string, c int);
insert into t1 (a, b, c) values
    (1, 'A', 1),
    (2, 'A', 2),
    (3, 'B', 1),
    (4, 'B', 2);

-- window function is explicitly defined in qualify clause and not included in select clause
explain cbo
select a, b, c
  from t1
qualify row_number() over (partition by b order by c) = 1;

select a, b, c
  from t1
qualify row_number() over (partition by b order by c) = 1;

-- window function is referenced by its alias in qualify clause and should be selected too
explain cbo
select a, b, c, row_number() over (partition by b order by c) as row_num
  from t1
qualify row_num = 1;

select a, b, c, row_number() over (partition by b order by c) as row_num
  from t1
qualify row_num = 1;

-- mixed form of the previous two
explain cbo
select a, b, c, row_number() over (partition by b order by c) as row_num
  from t1
qualify row_num = 1 and row_number() over (order by c) = 1;

select a, b, c, row_number() over (partition by b order by c) as row_num
  from t1
qualify row_num = 1 and row_number() over (order by c) = 1;
