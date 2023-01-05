set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.managed.tables=true;
set hive.create.as.acid=true;
set hive.create.as.insert.only=true;
set hive.default.fileformat.managed=ORC;

create table test_mm(empno int, name string) partitioned by(dept string) stored as orc;

-- CTAS with external legacy config
set hive.create.as.external.legacy=true;
create table test_external like test_mm;
desc formatted test_external;

create external table test_ext(empno int, name string) stored as ORC;

create table test_ext1 like test_ext;
desc formatted test_ext1;
