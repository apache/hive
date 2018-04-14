--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
-- SORT_BEFORE_DIFF

create table src_auth_tmp_n1 as select * from src;

create table authorization_part_n1 (key int, value string) partitioned by (ds string);
ALTER TABLE authorization_part_n1 SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="TRUE");
set hive.security.authorization.enabled=true;
grant select on table src_auth_tmp_n1 to user hive_test_user;

-- column grant to user
grant Create on table authorization_part_n1 to user hive_test_user;
grant Update on table authorization_part_n1 to user hive_test_user;
grant Drop on table authorization_part_n1 to user hive_test_user;

show grant user hive_test_user on table authorization_part_n1;
grant select(key) on table authorization_part_n1 to user hive_test_user;
insert overwrite table authorization_part_n1 partition (ds='2010') select key, value from src_auth_tmp_n1; 
show grant user hive_test_user on table authorization_part_n1(key) partition (ds='2010');
alter table authorization_part_n1 partition (ds='2010') rename to partition (ds='2010_tmp');
show grant user hive_test_user on table authorization_part_n1(key) partition (ds='2010_tmp');

drop table authorization_part_n1;
