set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=hive_admin_user;
set role admin;
create role r1;
grant role r1 to user r1user;

set user.name=user1;
CREATE TABLE  t1(i int);

-- all privileges should have been set for user

GRANT ALL ON t1 TO ROLE r1 WITH GRANT OPTION;

set user.name=r1user;
-- check if user belong to role r1 can grant privileges to others
GRANT ALL ON t1 TO USER user3;

set user.name=hive_admin_user;
set role admin;
-- check privileges on table
show grant on table t1;

-- check if drop role removes privileges for that role
drop role r1;
show grant on table t1;
