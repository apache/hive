set hive.mapred.mode=nonstrict;
SET hive.fetch.task.conversion=none;
SET hive.cbo.enable=false;
SET hive.map.aggr=false;
-- disabling map side aggregation as that can lead to different intermediate record counts

CREATE TABLE staging(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           dec decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE staging;
LOAD DATA LOCAL INPATH '../../data/files/over1k' INTO TABLE staging;

CREATE TABLE orc_ppd_staging(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           c char(50),
           v varchar(50),
           da date,
           dec decimal(4,2),
           bin binary)
STORED AS ORC tblproperties("orc.row.index.stride" = "1000", "orc.bloom.filter.columns"="*");

insert overwrite table orc_ppd_staging select t, si, i, b, f, d, bo, s, cast(s as char(50)), cast(s as varchar(50)), cast(ts as date), dec, bin from staging order by t, s;

-- just to introduce a gap in min/max range for bloom filters. The dataset has contiguous values
-- which makes it hard to test bloom filters
insert into orc_ppd_staging select -10,-321,-65680,-4294967430,-97.94,-13.07,true,"aaa","aaa","aaa","1990-03-11",-71.54,"aaa" from staging limit 1;
insert into orc_ppd_staging select 127,331,65690,4294967440,107.94,23.07,true,"zzz","zzz","zzz","2023-03-11",71.54,"zzz" from staging limit 1;

CREATE TABLE orc_ppd(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           c char(50),
           v varchar(50),
           da date,
           dec decimal(4,2),
           bin binary)
STORED AS ORC tblproperties("orc.row.index.stride" = "1000", "orc.bloom.filter.columns"="*");

insert overwrite table orc_ppd select t, si, i, b, f, d, bo, s, cast(s as char(50)), cast(s as varchar(50)), da, dec, bin from orc_ppd_staging order by t, s;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
SET hive.optimize.index.filter=false;

-- Row group statistics for column t:
-- Entry 0: count: 994 hasNull: true min: -10 max: 54 sum: 26014 positions: 0,0,0,0,0,0,0
-- Entry 1: count: 1000 hasNull: false min: 54 max: 118 sum: 86812 positions: 0,2,124,0,0,116,11
-- Entry 2: count: 100 hasNull: false min: 118 max: 127 sum: 12151 positions: 0,4,119,0,0,244,19

-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > 127;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > 127;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = 55;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = 55;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = 54;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = 54;

alter table orc_ppd change column t t smallint; 

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > 127;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > 127;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = 55;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = 55;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = 54;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = 54;

alter table orc_ppd change column t t int;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > 127;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > 127;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = 55;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = 55;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = 54;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = 54;

alter table orc_ppd change column t t bigint;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > 127;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > 127;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = 55;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = 55;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = 54;
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = 54;

alter table orc_ppd change column t t string;

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > '127';
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd where t > '127';

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = '55';
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd where t = '55';

SET hive.optimize.index.filter=false;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = '54';
SET hive.optimize.index.filter=true;
-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd where t = '54';

SET hive.optimize.index.filter=false;
-- float tests
select count(*) from orc_ppd where f = 74.72;
SET hive.optimize.index.filter=true;
select count(*) from orc_ppd where f = 74.72;

alter table orc_ppd change column f f double;

SET hive.optimize.index.filter=false;
select count(*) from orc_ppd where f = 74.72;
SET hive.optimize.index.filter=true;
select count(*) from orc_ppd where f = 74.72;

alter table orc_ppd change column f f string;

SET hive.optimize.index.filter=false;
select count(*) from orc_ppd where f = '74.72';
SET hive.optimize.index.filter=true;
select count(*) from orc_ppd where f = '74.72';

SET hive.optimize.index.filter=false;
-- string tests
select count(*) from orc_ppd where s = 'bob davidson';
SET hive.optimize.index.filter=true;
select count(*) from orc_ppd where s = 'bob davidson';

alter table orc_ppd change column s s char(50);

SET hive.optimize.index.filter=false;
select count(*) from orc_ppd where s = 'bob davidson';
SET hive.optimize.index.filter=true;
select count(*) from orc_ppd where s = 'bob davidson';

alter table orc_ppd change column s s varchar(50);

SET hive.optimize.index.filter=false;
select count(*) from orc_ppd where s = 'bob davidson';
SET hive.optimize.index.filter=true;
select count(*) from orc_ppd where s = 'bob davidson';

alter table orc_ppd change column s s char(50);

SET hive.optimize.index.filter=false;
select count(*) from orc_ppd where s = 'bob davidson';
SET hive.optimize.index.filter=true;
select count(*) from orc_ppd where s = 'bob davidson';

alter table orc_ppd change column s s string;

SET hive.optimize.index.filter=false;
select count(*) from orc_ppd where s = 'bob davidson';
SET hive.optimize.index.filter=true;
select count(*) from orc_ppd where s = 'bob davidson';

alter table orc_ppd add columns (boo boolean);

SET hive.optimize.index.filter=false;
-- ppd on newly added column
select count(*) from orc_ppd where si = 442;
select count(*) from orc_ppd where si = 442 or boo is not null or boo = false;
SET hive.optimize.index.filter=true;
select count(*) from orc_ppd where si = 442;
select count(*) from orc_ppd where si = 442 or boo is not null or boo = false;
