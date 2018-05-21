--! qt:dataset:src
drop table t_n24;
CREATE TABLE t_n24(c tinyint);
insert overwrite table t_n24 select 10 from src limit 1;

select * from t_n24 where c = 10.0;

select * from t_n24 where c = -10.0;