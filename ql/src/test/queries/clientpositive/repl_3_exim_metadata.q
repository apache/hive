set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=replsrc,repldst,repldst_md;
set hive.repl.run.data.copy.tasks.on.target=false;

drop table if exists replsrc;
drop table if exists repldst;
drop table if exists repldst_md;

create table replsrc (emp_id int comment "employee id")
        partitioned by (emp_country string, emp_state string)
        stored as textfile;
load data local inpath "../../data/files/test.dat"
        into table replsrc partition (emp_country="us",emp_state="ca");

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/repldst/temp;
dfs -rmr target/tmp/ql/test/data/exports/repldst;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/repldst_md/temp;
dfs -rmr target/tmp/ql/test/data/exports/repldst_md;

export table replsrc to 'ql/test/data/exports/repldst' for replication('repldst');
export table replsrc to 'ql/test/data/exports/repldst_md' for metadata replication('repldst md-only');

drop table replsrc;

import table repldst from 'ql/test/data/exports/repldst';
describe extended repldst;
show table extended like repldst;
show create table repldst;
select * from repldst;

-- should be similar, except that select will return no results
import table repldst_md from 'ql/test/data/exports/repldst_md';
describe extended repldst_md;
show table extended like repldst_md;
show create table repldst_md;
select * from repldst_md;

drop table repldst;
drop table repldst_md;

