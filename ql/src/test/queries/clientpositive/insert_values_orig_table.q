set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

drop table if exists acid_ivot_stage;
create table acid_ivot_stage(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    ctimestamp2 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN) stored as orc;
LOAD DATA LOCAL INPATH "../../data/files/alltypesorc" into table acid_ivot_stage;

create table acid_ivot(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    ctimestamp2 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN) clustered by (cint) into 1 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into acid_ivot select * from acid_ivot_stage;

select count(*) from acid_ivot;

insert into table acid_ivot values
        (1, 2, 3, 4, 3.14, 2.34, 'fred', 'bob', '2014-09-01 10:34:23.111', '1944-06-06 06:00:00', true, true),
        (111, 222, 3333, 444, 13.14, 10239302.34239320, 'fred', 'bob', '2014-09-01 10:34:23.111', '1944-06-06 06:00:00', true, true);

select count(*) from acid_ivot;

