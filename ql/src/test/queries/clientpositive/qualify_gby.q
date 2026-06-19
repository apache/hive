create table t1 (a int, b string, c int);
insert into t1 (a, b, c) values
    (1, 'A', 1),
    (1, 'A', 1),
    (2, 'A', 2),
    (3, 'B', 1),
    (4, 'B', 2);

-- window function is explicitly defined in qualify clause and not included in select clause

explain cbo
select a, b, c, count(*)
  from t1
group by a, b, c
qualify row_number() over (partition by b order by c) = 1;

select a, b, c, count(*)
  from t1
group by a, b, c
qualify row_number() over (partition by b order by c) = 1;
