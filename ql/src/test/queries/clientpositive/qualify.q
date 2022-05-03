create table t1 (a int, b string, c int);
insert into t1 (a, b, c) values
    (1, 'A', 1),
    (2, 'A', 2),
    (3, 'B', 1),
    (4, 'B', 2);

select a, b, c
  from t1
qualify row_number() over (partition by b order by c) = 1;
