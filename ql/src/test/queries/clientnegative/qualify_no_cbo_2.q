create table t1 (a int, b string, c int);

-- distribute by is not supported by cbo so this statement should fall back to non cbo path and fail
-- since qualify requires CBO
select a, b, c, row_number() over (partition by b order by c) as row_num
  from t1
qualify row_num = 1
distribute by a;
