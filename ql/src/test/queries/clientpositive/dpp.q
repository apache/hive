set hive.tez.dynamic.partition.pruning=true;

CREATE TABLE t1(key string, value string, c_int int, c_float float, c_boolean boolean) partitioned by (dt string);

CREATE TABLE t2(key string, value string, c_int int, c_float float, c_boolean boolean) partitioned by (dt string);

CREATE TABLE t3(key string, value string, c_int int, c_float float, c_boolean boolean) partitioned by (dt string);

 

insert into table t1 partition(dt='2018') values ('k1','v1',1,1.0,true);

insert into table t2 partition(dt='2018') values ('k2','v2',2,2.0,true);

insert into table t3 partition(dt='2018') values ('k3','v3',3,3.0,true);

 

CREATE VIEW `view1` AS select `t1`.`key`,`t1`.`value`,`t1`.`c_int`,`t1`.`c_float`,`t1`.`c_boolean`,`t1`.`dt` from `t1` union all select `t2`.`key`,`t2`.`value`,`t2`.`c_int`,`t2`.`c_float`,`t2`.`c_boolean`,`t2`.`dt` from `t2`;

CREATE VIEW `view2` AS select `t2`.`key`,`t2`.`value`,`t2`.`c_int`,`t2`.`c_float`,`t2`.`c_boolean`,`t2`.`dt` from `t2` union all select `t3`.`key`,`t3`.`value`,`t3`.`c_int`,`t3`.`c_float`,`t3`.`c_boolean`,`t3`.`dt` from `t3`;

create table t4 as select key,value,c_int,c_float,c_boolean,dt from t1 union all select v1.key,v1.value,v1.c_int,v1.c_float,v1.c_boolean,v1.dt from view1 v1 join view2 v2 on v1.dt=v2.dt;

CREATE VIEW `view3` AS select `t4`.`key`,`t4`.`value`,`t4`.`c_int`,`t4`.`c_float`,`t4`.`c_boolean`,`t4`.`dt` from `t4` union all select `t1`.`key`,`t1`.`value`,`t1`.`c_int`,`t1`.`c_float`,`t1`.`c_boolean`,`t1`.`dt` from `t1`;

 
explain
select count(0) from view2 v2 join view3 v3 on v2.dt=v3.dt;
select count(0) from view2 v2 join view3 v3 on v2.dt=v3.dt;
