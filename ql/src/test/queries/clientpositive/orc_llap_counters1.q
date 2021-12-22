set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
SET hive.optimize.index.filter=true;
SET hive.cbo.enable=false;
SET hive.vectorized.execution.enabled=true;
SET hive.llap.io.enabled=true;
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
           `dec` decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
TBLPROPERTIES ("hive.serialization.decode.binary.as.base64"="false");

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
           `dec` decimal(4,2),
           bin binary)
STORED AS ORC tblproperties("orc.row.index.stride" = "1000", "orc.bloom.filter.columns"="*");

insert overwrite table orc_ppd_staging select t, si, i, b, f, d, bo, s, cast(s as char(50)) as c,
cast(s as varchar(50)) as v, cast(ts as date) as da, `dec`, bin from staging order by t, si, i, b, f, d, bo, s, c, v, da, `dec`, bin;

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
           `dec` decimal(4,2),
           bin binary)
STORED AS ORC tblproperties("orc.row.index.stride" = "1000", "orc.bloom.filter.columns"="*");

insert overwrite table orc_ppd select t, si, i, b, f, d, bo, s, cast(s as char(50)) as c,
cast(s as varchar(50)) as v, da, `dec`, bin from orc_ppd_staging order by t, si, i, b, f, d, bo, s, c, v, da, `dec`, bin;


describe formatted orc_ppd;

SET hive.fetch.task.conversion=none;
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;

-- Row group statistics for column t:
-- Entry 0: count: 994 hasNull: true min: -10 max: 54 sum: 26014 positions: 0,0,0,0,0,0,0
-- Entry 1: count: 1000 hasNull: false min: 54 max: 118 sum: 86812 positions: 0,2,124,0,0,116,11
-- Entry 2: count: 100 hasNull: false min: 118 max: 127 sum: 12151 positions: 0,4,119,0,0,244,19

-- INPUT_RECORDS: 2100 (all row groups)
select count(*) from orc_ppd where t > -100;

-- 100% LLAP cache hit
select count(*) from orc_ppd where t > -100;

DROP TABLE staging;
DROP TABLE orc_ppd_staging;
DROP TABLE orc_ppd;
