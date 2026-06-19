set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;
-- current user has been set (comment line before the set cmd is resulting in parse error!!)

CREATE TABLE  t_auth_ins(i int);

CREATE TABLE t_select(i int);
GRANT ALL ON TABLE t_select TO ROLE public;

-- grant insert privilege to another user
GRANT INSERT ON t_auth_ins TO USER userWIns;
GRANT INSERT,DELETE ON t_auth_ins TO USER userWInsAndDel;

set user.name=hive_admin_user;
set role admin;
SHOW GRANT ON TABLE t_auth_ins;


set user.name=userWIns;
INSERT INTO TABLE t_auth_ins SELECT * from t_select;

set user.name=userWInsAndDel;
INSERT OVERWRITE TABLE t_auth_ins SELECT * from t_select;
