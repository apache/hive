--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

-- SORT_BEFORE_DIFF

create table src_autho_test_n2 as select * from src;

grant All on table src_autho_test_n2 to user hive_test_user;

set hive.security.authorization.enabled=true;

show grant user hive_test_user on table src_autho_test_n2;
GRANT drop ON DATABASE default TO USER hive_test_user;

select key from src_autho_test_n2 order by key limit 20;

drop table src_autho_test_n2;
