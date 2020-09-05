set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=replsrc,repldst;
set metastore.try.direct.sql=false;
set hive.metastore.rawstore.impl=org.apache.hadoop.hive.metastore.ObjectStore;
set hive.repl.run.data.copy.tasks.on.target=false;

drop table if exists replsrc;
drop table if exists repldst;

create table replsrc (emp_id int comment "employee id")
        partitioned by (emp_country string, emp_state string)
        stored as textfile;
load data local inpath "../../data/files/test.dat"
        into table replsrc partition (emp_country="us",emp_state="ca");

alter table replsrc add partition (emp_country="zx",emp_state="ka");

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/repldst/temp;
dfs -rmr target/tmp/ql/test/data/exports/repldst;

export table replsrc to 'ql/test/data/exports/repldst' for replication('repldst');

drop table replsrc;

import table repldst from 'ql/test/data/exports/repldst';
describe extended repldst;
show table extended like repldst;
show create table repldst;
select * from repldst;

drop table repldst;

