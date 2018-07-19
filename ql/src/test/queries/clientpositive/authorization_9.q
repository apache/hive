set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
-- SORT_BEFORE_DIFF

create database authorization_9;
use authorization_9;

create table dummy_n1 (key string, value string);

grant select to user hive_test_user;
grant select on database authorization_9 to user hive_test_user;
grant select on table dummy_n1 to user hive_test_user;
grant select (key, value) on table dummy_n1 to user hive_test_user;

show grant user hive_test_user on database authorization_9;
show grant user hive_test_user on table dummy_n1;
show grant user hive_test_user on all;

grant select to user hive_test_user2;
grant select on database authorization_9 to user hive_test_user2;
grant select on table dummy_n1 to user hive_test_user2;
grant select (key, value) on table dummy_n1 to user hive_test_user2;

show grant on all;
show grant user hive_test_user on all;
show grant user hive_test_user2 on all;

revoke select from user hive_test_user;
revoke select on database authorization_9 from user hive_test_user;
revoke select on table dummy_n1 from user hive_test_user;
revoke select (key, value) on table dummy_n1 from user hive_test_user;

revoke select from user hive_test_user2;
revoke select on database authorization_9 from user hive_test_user2;
revoke select on table dummy_n1 from user hive_test_user2;
revoke select (key, value) on table dummy_n1 from user hive_test_user2;
