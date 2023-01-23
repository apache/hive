-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/

set hive.explain.user=false;

create table source(a int) stored by iceberg tblproperties ('format-version'='2') ;

insert into source values (1), (2);

create table target like source stored by iceberg;

show create table target;

select count(*) from target;

insert into target values (1), (2);

select a from target order by a;

delete from target where a=2;

select a from target order by a;
