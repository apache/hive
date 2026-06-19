set hive.security.authorization.enabled=true;

explain create table if not exists authorization_explain (key int, value string);

create table if not exists authorization_explain (key int, value string);
revoke Select on table authorization_explain from user hive_test_user;
explain select * from authorization_explain;
