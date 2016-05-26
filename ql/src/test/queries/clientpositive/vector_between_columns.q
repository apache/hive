set hive.cli.print.header=true;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;

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

create table TINT stored as orc AS SELECT * FROM TINT_txt;


explain
select tint.rnum, tsint.rnum, tint.cint, tsint.csint, (case when (tint.cint between tsint.csint and tsint.csint) then "Ok" else "NoOk" end) as between_col from tint , tsint;

select tint.rnum, tsint.rnum, tint.cint, tsint.csint, (case when (tint.cint between tsint.csint and tsint.csint) then "Ok" else "NoOk" end) as between_col from tint , tsint;


explain
select tint.rnum, tsint.rnum, tint.cint, tsint.csint from tint , tsint where tint.cint between tsint.csint and tsint.csint;

select tint.rnum, tsint.rnum, tint.cint, tsint.csint from tint , tsint where tint.cint between tsint.csint and tsint.csint;
