create table authorization_fail_2 (key int, value string) partitioned by (ds string);

revoke `ALL` on table authorization_fail_2 from user hive_test_user;

set hive.security.authorization.enabled=true;

alter table authorization_fail_2 add partition (ds='2010');


