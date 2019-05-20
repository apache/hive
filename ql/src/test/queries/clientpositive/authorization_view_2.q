--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table src_autho_test_n13 as select * from src;

create view v1_n19 as select * from src_autho_test_n13;

create view v2_n12 as select * from v1_n19;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table v2_n12 to user hive_test_user;

select * from v2_n12 order by key limit 10;

