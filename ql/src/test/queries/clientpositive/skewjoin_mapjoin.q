create table t1(id int);
create table t2(id int);
create table t3(id int);
insert into table t1 values (1),(1),(1);
insert into table t2 values (1),(1);
insert into table t3 values (1);

set hive.auto.convert.join.noconditionaltask=false;
set hive.optimize.skewjoin=true;
set hive.auto.convert.join=true;
set hive.mapjoin.smalltable.filesize=1;
explain select count(a.id) from t1 a,t2 b,t3 c where a.id=b.id and a.id=c.id;
select count(a.id) from t1 a,t2 b,t3 c where a.id=b.id and a.id=c.id;
