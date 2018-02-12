set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc tblproperties("transactional"="false");
insert into table intermediate partition(p='455') select distinct key from src where key >= 0 order by key desc limit 2;
insert into table intermediate partition(p='456') select distinct key from src where key is not null order by key asc limit 2;
insert into table intermediate partition(p='457') select distinct key from src where key >= 100 order by key asc limit 2;

drop table intermediate_nonpart;
drop table intermmediate_part;
drop table intermmediate_nonpart;
create table intermediate_nonpart(key int, p int) tblproperties("transactional"="false");
insert into intermediate_nonpart select * from intermediate;
create table intermmediate_nonpart(key int, p int) tblproperties("transactional"="true", "transactional_properties"="insert_only");
insert into intermmediate_nonpart select * from intermediate;
create table intermmediate(key int) partitioned by (p int) tblproperties("transactional"="true", "transactional_properties"="insert_only");
insert into table intermmediate partition(p) select key, p from intermediate;

set hive.exim.test.mode=true;

export table intermediate_nonpart to 'ql/test/data/exports/intermediate_nonpart';
export table intermmediate_nonpart to 'ql/test/data/exports/intermmediate_nonpart';
export table intermediate to 'ql/test/data/exports/intermediate_part';
export table intermmediate to 'ql/test/data/exports/intermmediate_part';

drop table intermediate_nonpart;
drop table intermmediate_part;
drop table intermmediate_nonpart;

-- non-MM export to MM table, with and without partitions

drop table import0_mm;
create table import0_mm(key int, p int) tblproperties("transactional"="true", "transactional_properties"="insert_only");
import table import0_mm from 'ql/test/data/exports/intermediate_nonpart';
select * from import0_mm order by key, p;
drop table import0_mm;



drop table import1_mm;
create table import1_mm(key int) partitioned by (p int)
  stored as orc tblproperties("transactional"="true", "transactional_properties"="insert_only");
import table import1_mm from 'ql/test/data/exports/intermediate_part';
select * from import1_mm order by key, p;
drop table import1_mm;


-- MM export into new MM table, non-part and part

drop table import2_mm;
import table import2_mm from 'ql/test/data/exports/intermmediate_nonpart';
desc import2_mm;
select * from import2_mm order by key, p;
drop table import2_mm;

drop table import3_mm;
import table import3_mm from 'ql/test/data/exports/intermmediate_part';
desc import3_mm;
select * from import3_mm order by key, p;
drop table import3_mm;

-- MM export into existing MM table, non-part and partial part

drop table import4_mm;
create table import4_mm(key int, p int) tblproperties("transactional"="true", "transactional_properties"="insert_only");
import table import4_mm from 'ql/test/data/exports/intermmediate_nonpart';
select * from import4_mm order by key, p;
drop table import4_mm;

drop table import5_mm;
create table import5_mm(key int) partitioned by (p int) tblproperties("transactional"="true", "transactional_properties"="insert_only");
import table import5_mm partition(p=455) from 'ql/test/data/exports/intermmediate_part';
select * from import5_mm order by key, p;
drop table import5_mm;

-- MM export into existing non-MM table, non-part and part

drop table import6_mm;
create table import6_mm(key int, p int) tblproperties("transactional"="false");
import table import6_mm from 'ql/test/data/exports/intermmediate_nonpart';
select * from import6_mm order by key, p;
drop table import6_mm;

drop table import7_mm;
create table import7_mm(key int) partitioned by (p int) tblproperties("transactional"="false");
import table import7_mm from 'ql/test/data/exports/intermmediate_part';
select * from import7_mm order by key, p;
drop table import7_mm;

set hive.exim.test.mode=false;