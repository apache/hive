set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
-- SORT_BEFORE_DIFF

create table dummy (key string, value string);

grant select to user hive_test_user;
grant select on database default to user hive_test_user;
grant select on table dummy to user hive_test_user;
grant select (key, value) on table dummy to user hive_test_user;

show grant user hive_test_user on database default;
show grant user hive_test_user on table dummy;
show grant user hive_test_user on all;

grant select to user hive_test_user2;
grant select on database default to user hive_test_user2;
grant select on table dummy to user hive_test_user2;
grant select (key, value) on table dummy to user hive_test_user2;

show grant on all;
show grant user hive_test_user on all;
show grant user hive_test_user2 on all;

revoke select from user hive_test_user;
revoke select on database default from user hive_test_user;
revoke select on table dummy from user hive_test_user;
revoke select (key, value) on table dummy from user hive_test_user;

revoke select from user hive_test_user2;
revoke select on database default from user hive_test_user2;
revoke select on table dummy from user hive_test_user2;
revoke select (key, value) on table dummy from user hive_test_user2;
