set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.tez.auto.reducer.parallelism=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select distinct key from src where key >= 0 order by key desc limit 2;
insert into table intermediate partition(p='456') select distinct key from src where key is not null order by key asc limit 2;


drop table intermmediate_nonpart;
create table intermmediate_nonpart(key int, p int) tblproperties("transactional"="true", "transactional_properties"="insert_only");
insert into intermmediate_nonpart select * from intermediate;

set hive.exim.test.mode=true;

export table intermmediate_nonpart to 'ql/test/data/exports/intermmediate_nonpart';
drop table intermmediate_nonpart;

-- MM export into new MM table, non-part and part

drop table import2_mm;
import table import2_mm from 'ql/test/data/exports/intermmediate_nonpart';
desc import2_mm;
select * from import2_mm order by key, p;
drop table import2_mm;

drop table intermediate;


