set hive.cli.print.header=true;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;

-- SORT_QUERY_RESULTS

create table if not exists TSINT_txt ( RNUM int , CSINT smallint )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

create table if not exists TINT_txt ( RNUM int , CINT int )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

load data local inpath '../../data/files/TSINT' into table TSINT_txt;

load data local inpath '../../data/files/TINT' into table TINT_txt;

create table TSINT stored as orc AS SELECT * FROM TSINT_txt;

create table TINT stored as orc AS SELECT * FROM TINT_txt;

-- We DO NOT expect the following to vectorized because the BETWEEN range expressions
-- are not constants.  We currently do not support the range expressions being columns.
explain
select tint.rnum, tsint.rnum from tint , tsint where tint.cint between tsint.csint and tsint.csint;

select tint.rnum, tsint.rnum from tint , tsint where tint.cint between tsint.csint and tsint.csint;
