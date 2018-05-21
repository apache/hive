--! qt:dataset:src
set hive.cbo.enable=false;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table src_autho_test_n6 as select * from src;

create view v1_n10 as select * from src;

create view v2_n4 as select * from v1_n10;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table v2_n4 to user hive_test_user;

grant select(key) on table src_autho_test_n6 to user hive_test_user;

select v2_n4.key from v2_n4 join (select key from src_autho_test_n6)subq on v2_n4.value=subq.key order by key limit 10;

