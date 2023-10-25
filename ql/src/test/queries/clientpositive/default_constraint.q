--! qt:replace:/(totalSize\s+)(\S+|\s+|.+)/$1#Masked#/
-- create table
-- numeric type

-- MASK_DATA_SIZE

 set hive.stats.autogather=false;
 set hive.support.concurrency=true;
 set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE numericDataType_n1(a TINYINT CONSTRAINT tinyint_constraint DEFAULT 127Y ENABLE, b SMALLINT DEFAULT 32767S, c INT DEFAULT 2147483647,
    d BIGINT DEFAULT  9223372036854775807L, e DOUBLE DEFAULT 3.4E38, f DECIMAL(9,2) DEFAULT 1234567.89)
    clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
DESC FORMATTED numericDataType_n1;

EXPLAIN INSERT INTO numericDataType_n1(a) values(3Y);
INSERT INTO numericDataType_n1(a) values(3Y);
SELECT * FROM numericDataType_n1;

EXPLAIN INSERT INTO numericDataType_n1(e,f) values(4.5, 678.4);
INSERT INTO numericDataType_n1(e,f) values(4.5, 678.4);
SELECT * FROM numericDataType_n1;

DROP TABLE numericDataType_n1;

  -- Date/time
CREATE TABLE table1_n16(d DATE DEFAULT DATE'2018-02-14', t TIMESTAMP DEFAULT TIMESTAMP'2016-02-22 12:45:07.000000000',
    tz timestamp with local time zone DEFAULT TIMESTAMPLOCALTZ'2016-01-03 12:26:34 America/Los_Angeles',
    d1 DATE DEFAULT current_date() ENABLE, t1 TIMESTAMP DEFAULT current_timestamp() DISABLE);
DESC FORMATTED table1_n16;

EXPLAIN INSERT INTO table1_n16(t) values ("1985-12-31 12:45:07");
INSERT INTO table1_n16(t) values ("1985-12-31 12:45:07");
SELECT d, t, tz,d1=current_date(), t1 from table1_n16;

EXPLAIN INSERT INTO table1_n16(d, t1) values ("1985-12-31", '2018-02-27 17:32:14.259');
INSERT INTO table1_n16(d, t1) values ("1985-12-31", '2018-02-27 17:32:14.259');
SELECT d, t, tz,d1=current_date(), t1=current_timestamp() from table1_n16;

DROP TABLE table1_n16;

-- string type
CREATE TABLE table2_n11(i STRING DEFAULT 'current_database()', j STRING DEFAULT current_user(),
    k STRING DEFAULT 'Current_User()', v varchar(350) DEFAULT cast('varchar_default_value' as varchar(350)),
    c char(20) DEFAULT cast('char_value' as char(20)))
    clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
DESC FORMATTED table2_n11;
EXPLAIN INSERT INTO table2_n11(i) values('default');
INSERT INTO table2_n11(i) values('default');
SELECT i,j=current_user(),k,v,c FROM table2_n11;

EXPLAIN INSERT INTO table2_n11(v, c) values('varchar_default2', 'char');
INSERT INTO table2_n11(v, c) values('varchar_default2', 'char');
SELECT i,j=current_user(),k,v,c FROM table2_n11;
DROP TABLE table2_n11;


-- misc type
CREATE TABLE misc(b BOOLEAN DEFAULT true, b1 BINARY DEFAULT cast('bin' as binary))
    clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
DESC FORMATTED misc;
EXPLAIN INSERT INTO misc(b) values(false);
INSERT INTO misc(b) values(false);
SELECT b, b1 from misc;
EXPLAIN INSERT INTO misc(b1) values('011');
INSERT INTO misc(b) values(false);
SELECT b, b1 from misc;
DROP TABLE misc;

-- CAST
CREATE table t11_n2(i int default cast(cast(4 as double) as int),
    b1 boolean default cast ('true' as boolean), b2 int default cast (5.67 as int),
    b3 tinyint default cast (45 as tinyint), b4 float default cast (45.4 as float),
    b5 bigint default cast (567 as bigint), b6 smallint default cast (88 as smallint),
    j varchar(50) default cast(current_timestamp() as varchar(50)),
     k string default cast(cast(current_user() as varchar(50)) as string),
     tz1 timestamp with local time zone DEFAULT cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone),
     ts timestamp default cast('2016-01-01 12:01:01' as timestamp),
     dc decimal(8,2) default cast(4.5 as decimal(8,2)),
     c2 double default cast(5 as double), c4 char(2) default cast(cast(cast('ab' as string) as varchar(2)) as char(2)));
DESC FORMATTED t11_n2;
EXPLAIN INSERT INTO t11_n2(c4) values('vi');
INSERT INTO t11_n2(c4) values('vi');
SELECT ts, tz1, dc, b1,b2,b3,b4,b5,b6,j=cast(current_timestamp() as varchar(50)), k=cast(current_user() as string), c2, c4 from t11_n2;

EXPLAIN INSERT INTO t11_n2(b1,c4) values(true,'ga');
INSERT INTO t11_n2(c4) values('vi');
SELECT ts, tz1, dc, b1,b2,b3,b4,b5,b6,j=cast(current_timestamp() as varchar(50)), k=cast(current_user() as string), c2, c4 from t11_n2;

DROP TABLE t11_n2;

-- alter table
-- drop constraint
CREATE TABLE numericDataType_n1(a TINYINT CONSTRAINT tinyint_constraint DEFAULT 127Y ENABLE, b SMALLINT DEFAULT 32767S, c INT DEFAULT 2147483647,
    d BIGINT DEFAULT  9223372036854775807L, e DOUBLE DEFAULT 3.4E38, f DECIMAL(9,2) DEFAULT 1234567.89)
    clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
ALTER TABLE numericDataType_n1 DROP CONSTRAINT tinyint_constraint;
DESC FORMATTED numericDataType_n1;

EXPLAIN INSERT INTO numericDataType_n1(b) values(456);
INSERT INTO numericDataType_n1(b) values(456);
SELECT * from numericDataType_n1;

-- add another constraint on same column
ALTER TABLE numericDataType_n1 ADD CONSTRAINT uk1 UNIQUE(a,b) DISABLE NOVALIDATE;
DESC FORMATTED numericDataType_n1;
EXPLAIN INSERT INTO numericDataType_n1(b) values(56);
INSERT INTO numericDataType_n1(b) values(456);
SELECT * from numericDataType_n1;

-- alter table change column with constraint to add NOT NULL and then DEFAULT
ALTER TABLE numericDataType_n1 CHANGE a a TINYINT CONSTRAINT second_null_constraint NOT NULL ENABLE;
DESC FORMATTED numericDataType_n1;
ALTER TABLE numericDataType_n1 CHANGE a a TINYINT CONSTRAINT default_constraint DEFAULT 127Y ENABLE;
DESC FORMATTED numericDataType_n1;
EXPLAIN INSERT INTO numericDataType_n1(f) values(847.45); --plan should have both DEFAULT and NOT NULL
INSERT INTO numericDataType_n1(f) values(847.45);
Select * from numericDataType_n1;
DESC FORMATTED numericDataType_n1;

-- drop constraint and add with same name again
ALTER TABLE numericDataType_n1 DROP CONSTRAINT default_constraint;
DESC FORMATTED numericDataType_n1;
ALTER TABLE numericDataType_n1 CHANGE a a TINYINT CONSTRAINT default_constraint DEFAULT 108Y ENABLE;
DESC FORMATTED numericDataType_n1;
EXPLAIN INSERT INTO numericDataType_n1(f) values(847.45);
INSERT INTO numericDataType_n1(f) values(847.45);
Select * from numericDataType_n1;
DROP TABLE numericDataType_n1;

-- create default with maximum length allowed for default val (255)
create table t (i int, j string default
	'1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123');
desc formatted t;
explain insert into t(i) values(3);
insert into t(i) values(3);
select * from t;
drop table t;

-- partitioned table
-- Table with partition
CREATE TABLE tablePartitioned_n0 (a STRING NOT NULL ENFORCED, url STRING constraint bdc1 default 'http://localhost',
    c STRING NOT NULL ENFORCED)
    PARTITIONED BY (p1 STRING, p2 INT);

-- Insert into
explain INSERT INTO tablePartitioned_n0 partition(p1='today', p2=10) values('not', 'null', 'constraint');
INSERT INTO tablePartitioned_n0 partition(p1='today', p2=10) values('not', 'null', 'constraint');
DROP TABLE tablePartitioned_n0;

-- try constraint with direct sql as false
set hive.metastore.try.direct.sql=false;
CREATE TABLE numericDataType_n1(a TINYINT CONSTRAINT tinyint_constraint DEFAULT 127Y ENABLE, b SMALLINT DEFAULT 32767S, c INT DEFAULT 2147483647,
    d BIGINT DEFAULT  9223372036854775807L, e DOUBLE DEFAULT 3.4E38, f DECIMAL(9,2) DEFAULT 1234567.89)
    clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
ALTER TABLE numericDataType_n1 DROP CONSTRAINT tinyint_constraint;
DESC FORMATTED numericDataType_n1;

EXPLAIN INSERT INTO numericDataType_n1(b) values(456);
INSERT INTO numericDataType_n1(b) values(456);
SELECT * from numericDataType_n1;

-- add another constraint on same column
ALTER TABLE numericDataType_n1 ADD CONSTRAINT uk1 UNIQUE(a,b) DISABLE NOVALIDATE;
DESC FORMATTED numericDataType_n1;
EXPLAIN INSERT INTO numericDataType_n1(b) values(56);
INSERT INTO numericDataType_n1(b) values(456);
SELECT * from numericDataType_n1;
DROP TABLE numericDataType_n1;

-- Following all are existing BUGS
-- BUG1: alter table change constraint doesn't work, so following not working
-- ALTER TABLE numericDataType_n1 change a a TINYINT CONSTRAINT default_constraint DEFAULT 1Y ENABLE; -- change default val
-- ALTER TABLE numericDataType_n1 change a a TINYINT CONSTRAINT default_constraint_second DEFAULT 1Y ENABLE; -- change constraint name
-- ALTER TABLE numericDataType_n1 change a a TINYINT CONSTRAINT default_constraint_second DEFAULT 1Y DISABLE; -- DISABLE constraint
-- BUG2: ADD column not working
-- ALTER TABLE numericDataType_n1 add columns (dd double);
--BUG3: Following add multiple constraints
--ALTER TABLE numericDataType_n1 CHANGE c c INT DEFAULT cast(4.5 as INT);
-- BUG4 Replace column doesn't work, so following not workiing
-- alter table numericDataType_n1 replace columns (a TINYINT);
-- BUG5: select current_database() as default doesn't work

