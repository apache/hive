--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table src_autho_test as select * from src;

create view v as select * from src_autho_test;

set hive.security.authorization.enabled=true;

--table grant to user

grant select(key) on table src_autho_test to user hive_test_user;

select * from v order by key limit 1;
