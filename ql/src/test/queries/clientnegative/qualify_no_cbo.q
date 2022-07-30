set hive.cbo.enable=false;

create table t1 (a int, b string, c int);

select a, b, c, row_number() over (partition by b order by c) as row_num
  from t1
qualify row_num = 1;
