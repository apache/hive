--! qt:dataset:src
set hive.cbo.enable=false;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table src_autho_test_n10 as select * from src;

create view v1_n16 as select * from src_autho_test_n10;

create view v2_n9 as select * from v1_n16;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table v2_n9 to user hive_test_user;

select * from v2_n9 order by key limit 10;

