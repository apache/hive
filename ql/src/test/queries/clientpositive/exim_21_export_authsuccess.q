set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.test.mode=true;
set hive.test.mode.prefix=;

create table exim_department_n3 ( dep_id int) stored as textfile;
load data local inpath "../../data/files/test.dat" into table exim_department_n3;

set hive.security.authorization.enabled=true;

grant Select on table exim_department_n3 to user hive_test_user;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_department/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
export table exim_department_n3 to 'ql/test/data/exports/exim_department';

set hive.security.authorization.enabled=false;
drop table exim_department_n3;
