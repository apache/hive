set hive.auto.convert.join=false;

create table t_smj_left (key string, value int);

insert into t_smj_left values
('key1', 1),
('key1', 2);

create table t_smj_right (key string, value int);

insert into t_smj_right values
('key1', 1);

select
    t2.value
from t_smj_left t1
left join t_smj_right t2 on t1.key=t2.key and t1.value=2;
