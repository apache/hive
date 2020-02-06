--! qt:authorizer
set user.name=user1;

-- actions that require user to be table owner
create table t1_n108(i int);

ALTER TABLE t1_n108 SET SERDEPROPERTIES ('field.delim' = ',');
drop table t1_n108;

create table t1_n108(i int);
create view vt1_n0 as select * from t1_n108;

drop view vt1_n0;
alter table t1_n108 rename to tnew1;
