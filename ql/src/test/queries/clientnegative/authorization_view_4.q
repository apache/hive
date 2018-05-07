--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table src_autho_test as select * from src;

create view v as select * from src_autho_test;

create view v1 as select * from src_autho_test;

create view v2 as select * from src_autho_test;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table src_autho_test to user hive_test_user;

grant select on table v to user hive_test_user;
grant select on table v1 to user hive_test_user;
grant select(key) on table v2 to user hive_test_user;

select key from
(select value as key from v1 union select value as key from v2 union all select key from v)subq
limit 10;
