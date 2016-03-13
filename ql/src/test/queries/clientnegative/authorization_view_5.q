set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table src_autho_test as select * from src;

create view v1 as select * from src_autho_test;

create view v2 as select * from v1;

set hive.security.authorization.enabled=true;

--table not grant to user

--grant select on table v2 to user hive_test_user;

select * from v2 order by key limit 10;

