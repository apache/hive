--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

-- SORT_BEFORE_DIFF

create table src_autho_test_n5 as select * from src;

grant drop on table src_autho_test_n5 to user hive_test_user;
grant select on table src_autho_test_n5 to user hive_test_user;

show grant user hive_test_user on table src_autho_test_n5;

revoke select on table src_autho_test_n5 from user hive_test_user;
revoke drop on table src_autho_test_n5 from user hive_test_user;

grant drop,select on table src_autho_test_n5 to user hive_test_user;
show grant user hive_test_user on table src_autho_test_n5;
revoke drop,select on table src_autho_test_n5 from user hive_test_user;

grant drop,select(key), select(value) on table src_autho_test_n5 to user hive_test_user;
show grant user hive_test_user on table src_autho_test_n5;
revoke drop,select(key), select(value) on table src_autho_test_n5 from user hive_test_user;
