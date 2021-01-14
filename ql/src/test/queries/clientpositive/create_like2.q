-- Tests the copying over of Table Parameters according to a HiveConf setting
-- when doing a CREATE TABLE LIKE.

CREATE TABLE table1_n20(a INT, b STRING);
ALTER TABLE table1_n20 SET TBLPROPERTIES ('a'='1', 'b'='2', 'c'='3', 'd' = '4');

SET hive.ddl.createtablelike.properties.whitelist=a,c,D;
CREATE TABLE table2_n14 LIKE table1_n20;
DESC FORMATTED table2_n14;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.managed.tables=true;
set hive.create.as.acid=true;
set hive.create.as.insert.only=true;
set hive.default.fileformat.managed=ORC;

create table test_mm(empno int, name string) partitioned by(dept string) stored as orc tblproperties('transactional'='true', 'transactional_properties'='default');
desc formatted test_mm;

-- Conversion from MM to External
create external table test_external like test_mm LOCATION '${system:test.tmp.dir}/create_like_mm_to_external';
desc formatted test_external;

-- Conversion from External to MM
create table test_mm1 like test_external;
desc formatted test_mm1;

-- Conversion from External to External
create external table test_external1 like test_external;
desc formatted test_external1;

-- Conversion from mm to mm
create table test_mm2 like test_mm;
desc formatted test_mm2;

drop table test_mm;
drop table test_external;
drop table test_mm1;
drop table test_external1;
drop table test_mm2;
