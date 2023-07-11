set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=false;

Create database db_auth_fail_1;

use db_auth_fail_1;

create table auth_fail_1 (key int, value string) partitioned by (ds string);

grant All on table auth_fail_1 to user hive_test_user;

set hive.security.authorization.enabled=true;

show grant user hive_test_user on table auth_fail_1;

-- Drop table should fail as drop privilege to the database is not added 
drop table if exists auth_fail_1;
