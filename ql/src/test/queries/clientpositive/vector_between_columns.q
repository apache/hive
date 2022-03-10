set hive.cli.print.header=true;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;
set hive.join.inner.residual=false;
set hive.optimize.index.filter=true;

-- SORT_QUERY_RESULTS
--
-- Verify the VectorUDFAdaptor to GenericUDFBetween works for PROJECTION and FILTER.
--
create table if not exists TSINT_txt ( RNUM int , CSINT smallint )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

create table if not exists TINT_txt ( RNUM int , CINT int )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

load data local inpath '../../data/files/TSINT' into table TSINT_txt;

load data local inpath '../../data/files/TINT' into table TINT_txt;

create table TSINT stored as orc AS SELECT * FROM TSINT_txt;

-- Add a single NULL row that will come from ORC as isRepeated.
insert into TSINT values (NULL, NULL);

create table TINT stored as orc AS SELECT * FROM TINT_txt;

-- Add a single NULL row that will come from ORC as isRepeated.
insert into TINT values (NULL, NULL);

CREATE EXTERNAL TABLE test_orc_ppd(
  data_release bigint,
  data_owner_ver_id bigint,
  data_owner_dim_id bigint,
  data_source_ver_id bigint,
  data_source_dim_id bigint,
  data_client_ver_id bigint,
  data_client_dim_id bigint,
  data_client_sub_ver_id bigint,
  data_client_sub_dim_id bigint,
  quarter_dim_id bigint,
  market_dim_id bigint,
  daypart_dim_id bigint,
  demo_dim_id bigint,
  station_dim_id bigint,
  medium_dim_id bigint,
  ad_length int,
  exclude int,
  population int,
  client_cpp double,
  client_cpm double,
  low_cpp double,
  mid_cpp double,
  high_cpp double,
  low_cpm double,
  mid_cpm double,
  high_cpm double,
  low_cpp_index double,
  mid_cpp_index double,
  high_cpp_index double,
  low_cpm_index double,
  mid_cpm_index double,
  high_cpm_index double)
  STORED AS ORC;
LOAD DATA LOCAL INPATH '../../data/files/orc_test_ppd'
OVERWRITE INTO TABLE test_orc_ppd;

explain vectorization expression
select tint.rnum, tsint.rnum, tint.cint, tsint.csint, (case when (tint.cint between tsint.csint and tsint.csint) then "Ok" else "NoOk" end) as between_col from tint , tsint;

select tint.rnum, tsint.rnum, tint.cint, tsint.csint, (case when (tint.cint between tsint.csint and tsint.csint) then "Ok" else "NoOk" end) as between_col from tint , tsint;


explain vectorization expression
select tint.rnum, tsint.rnum, tint.cint, tsint.csint from tint , tsint where tint.cint between tsint.csint and tsint.csint;

select tint.rnum, tsint.rnum, tint.cint, tsint.csint from tint , tsint where tint.cint between tsint.csint and tsint.csint;

explain vectorization expression
select data_release, count(*) from test_orc_ppd where NOT (data_release BETWEEN 20191201 AND 20200101) group by data_release;

select data_release, count(*) from test_orc_ppd where NOT (data_release BETWEEN 20191201 AND 20200101) group by data_release;
