drop table t;
CREATE TABLE t(c tinyint);
insert overwrite table t select 10 from src limit 1;

select * from t where c = 10.0;

select * from t where c = -10.0;