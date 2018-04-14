--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.compute.query.using.stats=true;

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

desc formatted acid_ivot;

insert into acid_ivot select * from acid_ivot_stage;

desc formatted acid_ivot;

explain select count(*) from acid_ivot;

select count(*) from acid_ivot;

insert into table acid_ivot values
        (1, 2, 3, 4, 3.14, 2.34, 'fred', 'bob', '2014-09-01 10:34:23.111', '1944-06-06 06:00:00', true, true),
        (111, 222, 3333, 444, 13.14, 10239302.34239320, 'fred', 'bob', '2014-09-01 10:34:23.111', '1944-06-06 06:00:00', true, true);

select count(*) from acid_ivot;

drop table acid_ivot;

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

insert into table acid_ivot values
        (1, 2, 3, 4, 3.14, 2.34, 'fred', 'bob', '2014-09-01 10:34:23.111', '1944-06-06 06:00:00', true, true),
        (111, 222, 3333, 444, 13.14, 10239302.34239320, 'fred', 'bob', '2014-09-01 10:34:23.111', '1944-06-06 06:00:00', true, true);

desc formatted acid_ivot;

explain select count(*) from acid_ivot;

select count(*) from acid_ivot;

insert into table acid_ivot values
        (1, 2, 3, 4, 3.14, 2.34, 'fred', 'bob', '2014-09-01 10:34:23.111', '1944-06-06 06:00:00', true, true),
        (111, 222, 3333, 444, 13.14, 10239302.34239320, 'fred', 'bob', '2014-09-01 10:34:23.111', '1944-06-06 06:00:00', true, true);

desc formatted acid_ivot;

explain select count(*) from acid_ivot;

select count(*) from acid_ivot;

insert into acid_ivot select * from acid_ivot_stage;

desc formatted acid_ivot;

explain select count(*) from acid_ivot;

drop table acid_ivot;

create table acid_ivot like src;

desc formatted acid_ivot;

insert overwrite table acid_ivot select * from src;

desc formatted acid_ivot;

explain select count(*) from acid_ivot;

select count(*) from acid_ivot;

CREATE TABLE sp (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/kv1.txt"
OVERWRITE INTO TABLE sp PARTITION (ds="2008-04-08", hr="11");

desc formatted sp PARTITION (ds="2008-04-08", hr="11");

explain select count(*) from sp where ds="2008-04-08" and hr="11";

select count(*) from sp where ds="2008-04-08" and hr="11";

insert into table sp PARTITION (ds="2008-04-08", hr="11") values
        ('1', '2'), ('3', '4');

desc formatted sp PARTITION (ds="2008-04-08", hr="11");

analyze table sp PARTITION (ds="2008-04-08", hr="11") compute statistics;

desc formatted sp PARTITION (ds="2008-04-08", hr="11");

explain select count(*) from sp where ds="2008-04-08" and hr="11";

select count(*) from sp where ds="2008-04-08" and hr="11";

