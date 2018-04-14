--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

-- SORT_BEFORE_DIFF

create table src_auth_tmp_n0 as select * from src;

create table authorization_part_n0 (key int, value string) partitioned by (ds string);
ALTER TABLE authorization_part_n0 SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="TRUE");
set hive.security.authorization.enabled=true;
grant select on table src_auth_tmp_n0 to user hive_test_user;

-- column grant to user
grant Create on table authorization_part_n0 to user hive_test_user;
grant Update on table authorization_part_n0 to user hive_test_user;
grant Drop on table authorization_part_n0 to user hive_test_user;

show grant user hive_test_user on table authorization_part_n0;
grant select(key) on table authorization_part_n0 to user hive_test_user;
insert overwrite table authorization_part_n0 partition (ds='2010') select key, value from src_auth_tmp_n0;
insert overwrite table authorization_part_n0 partition (ds='2011') select key, value from src_auth_tmp_n0; 
show grant user hive_test_user on table authorization_part_n0(key) partition (ds='2010');
show grant user hive_test_user on table authorization_part_n0(key) partition (ds='2011');
show grant user hive_test_user on table authorization_part_n0(key);
select key from authorization_part_n0 where ds>='2010' order by key limit 20;

drop table authorization_part_n0;

set hive.security.authorization.enabled=false;
create table authorization_part_n0 (key int, value string) partitioned by (ds string);
ALTER TABLE authorization_part_n0 SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="FALSE");

set hive.security.authorization.enabled=true;
grant Create on table authorization_part_n0 to user hive_test_user;
grant Update on table authorization_part_n0 to user hive_test_user;

show grant user hive_test_user on table authorization_part_n0;

grant select(key) on table authorization_part_n0 to user hive_test_user;
insert overwrite table authorization_part_n0 partition (ds='2010') select key, value from src_auth_tmp_n0;
insert overwrite table authorization_part_n0 partition (ds='2011') select key, value from src_auth_tmp_n0; 
show grant user hive_test_user on table authorization_part_n0(key) partition (ds='2010');
show grant user hive_test_user on table authorization_part_n0(key) partition (ds='2011');
show grant user hive_test_user on table authorization_part_n0(key);
select key from authorization_part_n0 where ds>='2010' order by key limit 20;
