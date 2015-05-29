set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=managed_t,ext_t,managed_t_imported,managed_t_r_imported,ext_t_imported,ext_t_r_imported;

drop table if exists managed_t;
drop table if exists ext_t;
drop table if exists managed_t_imported;
drop table if exists managed_t_r_imported;
drop table if exists ext_t_imported;
drop table if exists ext_t_r_imported;

create table managed_t (emp_id int comment "employee id")
        partitioned by (emp_country string, emp_state string)
        stored as textfile;
load data local inpath "../../data/files/test.dat"
        into table managed_t partition (emp_country="us",emp_state="ca");

create external table ext_t (emp_id int comment "employee id")
        partitioned by (emp_country string, emp_state string)
        stored as textfile
        tblproperties("EXTERNAL"="true");
load data local inpath "../../data/files/test.dat"
        into table ext_t partition (emp_country="us",emp_state="ca");

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/managed_t/temp;
dfs -rmr target/tmp/ql/test/data/exports/managed_t;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/managed_t_r/temp;
dfs -rmr target/tmp/ql/test/data/exports/managed_t_r;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/ext_t/temp;
dfs -rmr target/tmp/ql/test/data/exports/ext_t;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/ext_t_r/temp;
dfs -rmr target/tmp/ql/test/data/exports/ext_t_r;

-- verifying difference between normal export of a external table
-- and a replication export of an ext table
-- the replication export will have squashed the "EXTERNAL" flag
-- this is because the destination of all replication exports are
-- managed tables. The managed tables should be similar except
-- for the repl.last.id values

export table managed_t to 'ql/test/data/exports/managed_t';
export table managed_t to 'ql/test/data/exports/managed_t_r' for replication('managed_t_r');
export table ext_t to 'ql/test/data/exports/ext_t';
export table ext_t to 'ql/test/data/exports/ext_t_r' for replication('ext_t_r');

drop table ext_t;
drop table managed_t;

import table managed_t_imported from 'ql/test/data/exports/managed_t';
describe extended managed_t_imported;
show table extended like managed_t_imported;
show create table managed_t_imported;
select * from managed_t_imported;

-- should have repl.last.id
import table managed_t_r_imported from 'ql/test/data/exports/managed_t_r';
describe extended managed_t_r_imported;
show table extended like managed_t_r_imported;
show create table managed_t_r_imported;
select * from managed_t_r_imported;

import table ext_t_imported from 'ql/test/data/exports/ext_t';
describe extended ext_t_imported;
show table extended like ext_t_imported;
show create table ext_t_imported;
select * from ext_t_imported;

-- should have repl.last.id
-- also - importing an external table replication export would turn the new table into a managed table
import table ext_t_r_imported from 'ql/test/data/exports/ext_t_r';
describe extended ext_t_imported;
show table extended like ext_t_r_imported;
show create table ext_t_r_imported;
select * from ext_t_r_imported;

drop table managed_t_imported;
drop table managed_t_r_imported;
drop table ext_t_imported;
drop table ext_t_r_imported;
