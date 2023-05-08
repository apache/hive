--! qt:dataset:alltypesorc
--! qt:replace:/(\s+HDFS_BYTES_WRITTEN:\s+)\S+(\s*)/$1#Masked#/
--! qt:replace:/(\s+HDFS_BYTES_READ:\s+)\S+(\s*)/$1#Masked#/
SET hive.vectorized.execution.enabled=false;
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
SET hive.fetch.task.conversion=none;
SET hive.optimize.index.filter=true;
SET hive.cbo.enable=false;
SET hive.map.aggr=false;
-- disabling map side aggregation as that can lead to different intermediate record counts

CREATE TABLE staging_n7(t tinyint,
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
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE staging_n7;
LOAD DATA LOCAL INPATH '../../data/files/over1k' INTO TABLE staging_n7;

CREATE TABLE orc_ppd_staging_n1(t tinyint,
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

insert overwrite table orc_ppd_staging_n1 select t, si, i, b, f, d, bo, s, cast(s as char(50)) as c,
cast(s as varchar(50)) as v, cast(ts as date) as da, `dec`, bin from staging_n7 order by t, si, i, b, f, d, bo, s, c, v, da, `dec`, bin;

-- just to introduce a gap in min/max range for bloom filters. The dataset has contiguous values
-- which makes it hard to test bloom filters
insert into orc_ppd_staging_n1 select -10,-321,-65680,-4294967430,-97.94,-13.07,true,"aaa","aaa","aaa","1990-03-11",-71.54,"aaa" from staging_n7 limit 1;
insert into orc_ppd_staging_n1 select 127,331,65690,4294967440,107.94,23.07,true,"zzz","zzz","zzz","2023-03-11",71.54,"zzz" from staging_n7 limit 1;

CREATE TABLE orc_ppd_n2(t tinyint,
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

insert overwrite table orc_ppd_n2 select t, si, i, b, f, d, bo, s, cast(s as char(50)) as c,
cast(s as varchar(50)) as v, da, `dec`, bin from orc_ppd_staging_n1 order by t, si, i, b, f, d, bo, s, c, v, da, `dec`, bin;


SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;

-- Row group statistics for column t:
-- Entry 0: count: 994 hasNull: true min: -10 max: 54 sum: 26014 positions: 0,0,0,0,0,0,0
-- Entry 1: count: 1000 hasNull: false min: 54 max: 118 sum: 86812 positions: 0,2,124,0,0,116,11
-- Entry 2: count: 100 hasNull: false min: 118 max: 127 sum: 12151 positions: 0,4,119,0,0,244,19

-- INPUT_RECORDS: 2100 (all row groups)
select count(*) from orc_ppd_n2;

-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd_n2 where t > 127;

-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd_n2 where t = 55;
select count(*) from orc_ppd_n2 where t <=> 50;
select count(*) from orc_ppd_n2 where t <=> 100;

-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd_n2 where t = "54";

-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd_n2 where t = -10.0;

-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd_n2 where t = cast(53 as float);
select count(*) from orc_ppd_n2 where t = cast(53 as double);

-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd_n2 where t < 100;

-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd_n2 where t < 100 and t > 98;

-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd_n2 where t <= 100;

-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd_n2 where t is null;

-- INPUT_RECORDS: 1100 (2 row groups)
select count(*) from orc_ppd_n2 where t in (5, 120);

-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd_n2 where t between 60 and 80;

-- bloom filter tests
-- INPUT_RECORDS: 0
select count(*) from orc_ppd_n2 where t = -100;
select count(*) from orc_ppd_n2 where t <=> -100;
select count(*) from orc_ppd_n2 where t = 125;
select count(*) from orc_ppd_n2 where t IN (-100, 125, 200);

-- Row group statistics for column s:
-- Entry 0: count: 1000 hasNull: false min:  max: zach young sum: 12907 positions: 0,0,0
-- Entry 1: count: 1000 hasNull: false min: alice allen max: zach zipper sum: 12704 positions: 0,1611,191
-- Entry 2: count: 100 hasNull: false min: bob davidson max: zzz sum: 1281 positions: 0,3246,373

-- INPUT_RECORDS: 0 (no row groups)
select count(*) from orc_ppd_n2 where s > "zzz";

-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd_n2 where s = "zach young";
select count(*) from orc_ppd_n2 where s <=> "zach zipper";
select count(*) from orc_ppd_n2 where s <=> "";

-- INPUT_RECORDS: 0
select count(*) from orc_ppd_n2 where s is null;

-- INPUT_RECORDS: 2100
select count(*) from orc_ppd_n2 where s is not null;

-- INPUT_RECORDS: 0
select count(*) from orc_ppd_n2 where s = cast("zach young" as char(50));

-- INPUT_RECORDS: 1000 (1 row group)
select count(*) from orc_ppd_n2 where s = cast("zach young" as char(10));
select count(*) from orc_ppd_n2 where s = cast("zach young" as varchar(10));
select count(*) from orc_ppd_n2 where s = cast("zach young" as varchar(50));

-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd_n2 where s < "b";

-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd_n2 where s > "alice" and s < "bob";

-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd_n2 where s in ("alice allen", "");

-- INPUT_RECORDS: 2000 (2 row groups)
select count(*) from orc_ppd_n2 where s between "" and "alice allen";

-- INPUT_RECORDS: 100 (1 row group)
select count(*) from orc_ppd_n2 where s between "zz" and "zzz";

-- INPUT_RECORDS: 1100 (2 row groups)
select count(*) from orc_ppd_n2 where s between "zach zipper" and "zzz";

-- bloom filter tests
-- INPUT_RECORDS: 0
select count(*) from orc_ppd_n2 where s = "hello world";
select count(*) from orc_ppd_n2 where s <=> "apache hive";
select count(*) from orc_ppd_n2 where s IN ("a", "z");

-- INPUT_RECORDS: 100
select count(*) from orc_ppd_n2 where s = "sarah ovid";

-- INPUT_RECORDS: 1100
select count(*) from orc_ppd_n2 where s = "wendy king";

-- INPUT_RECORDS: 1000
select count(*) from orc_ppd_n2 where s = "wendy king" and t < 0;

-- INPUT_RECORDS: 100
select count(*) from orc_ppd_n2 where s = "wendy king" and t > 100;

set hive.cbo.enable=false;
set hive.optimize.index.filter=false;
-- when cbo is disabled constant gets converted to HiveDecimal
--  74.72f + 0.0 = 74.72000122070312
select count(*) from orc_ppd_n2 where f=74.72;
set hive.optimize.index.filter=true;
select count(*) from orc_ppd_n2 where f=74.72;

set hive.cbo.enable=true;
set hive.optimize.index.filter=false;
select count(*) from orc_ppd_n2 where f=74.72;
set hive.optimize.index.filter=true;
select count(*) from orc_ppd_n2 where f=74.72;

-- 42.47f + 0.0 == 42.470001220703125
create temporary table orc_ppd_1 stored as orc as select * from orc_ppd_staging_n1 where d = 42.47;

set hive.cbo.enable=false;
set hive.optimize.index.filter=false;
-- when cbo is disabled constant gets converted to HiveDecimal
select count(*) from orc_ppd_1 where d=42.47;
set hive.optimize.index.filter=true;
select count(*) from orc_ppd_1 where d=42.47;

set hive.cbo.enable=true;
set hive.optimize.index.filter=false;
select count(*) from orc_ppd_1 where d=42.47;
set hive.optimize.index.filter=true;
select count(*) from orc_ppd_1 where d=42.47;

RESET;
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
SET hive.fetch.task.conversion=none;
SET hive.optimize.index.filter=true;
SET hive.cbo.enable=false;
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcRowGroupCountPrinter;
-- these tests include timestamp column that will impact the file size when tests run across
-- different timezones. So we print only the selected row group count instead of entire tez exeuction summary.
create temporary table tmp_orcppd
                    stored as orc
                    as select ctinyint, csmallint, cint , cbigint, cfloat, cdouble,
                              cstring1, cstring2, ctimestamp1, ctimestamp2
                    from alltypesorc limit 20;
insert into table tmp_orcppd
                    values(null, null, null, null, null,
                           null, null, null, null, null);

drop table if exists tbl_orcppd_1_1;

create table tbl_orcppd_1_1 as
    select count(*) from tmp_orcppd
            where ctimestamp1> current_timestamp() and
            ctimestamp2 > current_timestamp() and
            cstring1 like 'a*' and
            cstring2 like 'a*';

drop table if exists tmp_orcppd;

create temporary table tmp_orcppd
                    stored as orc
                    as select ctimestamp1, ctimestamp2
                    from alltypesorc limit 10;
insert into table tmp_orcppd
                    values(null,  null);

drop table if exists tbl_orcppd_2_1;
create table tbl_orcppd_2_1 as
        select count(*) from tmp_orcppd
                    where ctimestamp1 in (cast('2065-08-13 19:03:52' as timestamp), cast('2071-01-16 20:21:17' as timestamp), current_timestamp());
set hive.optimize.index.filter=true;

drop table if exists tmp_orcppd;
create temporary table tmp_orcppd
                    stored as orc
                    as select ts, cast(ts as date)
                    from staging_n7 ;
insert into table tmp_orcppd
                    values(null, null);

drop table if exists tbl_orcppd_3_1;
create table tbl_orcppd_3_1 as
   select count(*) from tmp_orcppd
            group by ts, cast(ts as date)
            having ts in (select ctimestamp1 from alltypesorc limit 10);
