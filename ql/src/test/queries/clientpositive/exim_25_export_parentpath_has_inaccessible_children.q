set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set hive.test.mode=true;
set hive.test.mode.prefix=;

create table t_exppath ( dep_id int) stored as textfile;
-- this test tests HIVE-10022, by showing that we can output to a directory that does not yet exist
-- even if we do not have permissions to other subdirs of the parent dir

load data local inpath "../../data/files/test.dat" into table t_exppath;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/t_exppath/nope;
dfs -rmr ${system:test.tmp.dir}/t_exppath/;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/t_exppath/;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/t_exppath/nope;
dfs -chmod -R 400 ${system:test.tmp.dir}/t_exppath/nope;

set hive.security.authorization.enabled=true;

GRANT ALL on TABLE t_exppath to ROLE public;
export table t_exppath to '${system:test.tmp.dir}/t_exppath/yup';

set hive.security.authorization.enabled=false;
dfs -chmod -R 777 ${system:test.tmp.dir}/t_exppath/nope;
drop table t_exppath;
