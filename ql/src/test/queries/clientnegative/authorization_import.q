--! qt:authorizer

set test.hive.authz.sstd.validator.bypassObjTypes=DFS_URI;

set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=import_auth_t1,import_auth_t2,import_auth_t3;

drop table if exists import_auth_t1;
create table import_auth_t1 ( dep_id int comment "department id") stored as textfile;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/import_auth_t1/temp;
dfs -rmr target/tmp/ql/test/data/exports/import_auth_t1;

export table import_auth_t1 to 'ql/test/data/exports/import_auth_t1';

dfs -touchz target/tmp/ql/test/data/exports/import_auth_t1/1.txt;
dfs -chmod 777 target/tmp/ql/test/data/exports/import_auth_t1/1.txt;
dfs -chmod 777 target/tmp/ql/test/data/exports/import_auth_t1;

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/import_auth_t2/temp;

set user.name=hive_admin_user;
set role admin;

create database importer;
use importer;

show roles;

set user.name=hive_test_user;
set role public;

import table import_auth_t2 from 'ql/test/data/exports/import_auth_t1';

set user.name=hive_admin_user;
set role admin;
