set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

-- the main goal of these tests is to run a simple load and a load with regex, while being in the scope of SQLStdHiveAuthorizer

create table t_auth_load(key string, value string) stored as textfile;
create table t_auth_load2(key string, value string) stored as textfile;

GRANT ALL on TABLE t_auth_load to ROLE public;
GRANT ALL on TABLE t_auth_load2 to ROLE public;

load data local inpath '../../data/files/kv1.txt' into table t_auth_load;
load data local inpath '../../data/files/kv2.txt' into table t_auth_load;
load data local inpath '../../data/files/kv3.txt' into table t_auth_load;

show table extended like t_auth_load;
desc extended t_auth_load;

load data local inpath '../../data/files/kv[123].tx*' into table t_auth_load2;

show table extended like t_auth_load2;
desc extended t_auth_load2;

-- the following two selects should be identical
select count(*) from t_auth_load;
select count(*) from t_auth_load2;



