--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table src_autho_test_n1 as select * from src;

create view v1_n2 as select * from src_autho_test_n1;

create view v2_n0 as select * from v1_n2;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table v2_n0 to user hive_test_user;

grant select(key) on table src_autho_test_n1 to user hive_test_user;

select v2_n0.key from v2_n0 join (select key from src_autho_test_n1)subq on v2_n0.value=subq.key order by key limit 10;

