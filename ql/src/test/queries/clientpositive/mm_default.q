
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


set hive.create.as.insert.only=false;
create table non_mm0 as select key from src limit 1;
create table mm0 tblproperties("transactional"="true", "transactional_properties"="insert_only")
 as select key from src limit 1;
create table acid0 (key string) stored as ORC  tblproperties("transactional"="true");


set hive.create.as.insert.only=true;
create table mm1 like non_mm0;
create table mm2 like mm0;
create table acid1 like acid0;
create table mm3 as select key from src limit 1;
create table mm4 (key string);
create table acid2 (key string) stored as ORC  tblproperties("transactional"="true");
create table non_mm1 tblproperties("transactional"="false")
 as select key from src limit 1;

set hive.create.as.insert.only=false;

desc formatted mm1;
desc formatted mm2;
desc formatted mm3;
desc formatted mm4;
desc formatted non_mm1;
desc formatted acid1;
desc formatted acid2;


drop table non_mm0;
drop table non_mm1;
drop table mm0;
drop table mm1;
drop table mm2;
drop table mm3;
drop table mm4;
drop table acid0;
drop table acid1;
drop table acid2;




