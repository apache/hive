--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
set hive.optimize.metadataonly=true;
CREATE TABLE TEST1_n12(A INT, B DOUBLE) partitioned by (ds string);
explain extended select max(ds) from TEST1_n12;
select max(ds) from TEST1_n12;

alter table TEST1_n12 add partition (ds='1');
explain extended select max(ds) from TEST1_n12;
select max(ds) from TEST1_n12;

explain extended select count(distinct ds) from TEST1_n12;
select count(distinct ds) from TEST1_n12;

explain extended select count(ds) from TEST1_n12;
select count(ds) from TEST1_n12;

alter table TEST1_n12 add partition (ds='2');
explain extended 
select count(*) from TEST1_n12 a2 join (select max(ds) m from TEST1_n12) b on a2.ds=b.m;
select count(*) from TEST1_n12 a2 join (select max(ds) m from TEST1_n12) b on a2.ds=b.m;


CREATE TABLE TEST2_n8(A INT, B DOUBLE) partitioned by (ds string, hr string);
alter table TEST2_n8 add partition (ds='1', hr='1');
alter table TEST2_n8 add partition (ds='1', hr='2');
alter table TEST2_n8 add partition (ds='1', hr='3');

explain extended select ds, count(distinct hr) from TEST2_n8 group by ds;
select ds, count(distinct hr) from TEST2_n8 group by ds;

explain extended select ds, count(hr) from TEST2_n8 group by ds;
select ds, count(hr) from TEST2_n8 group by ds;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

explain extended select max(ds) from TEST1_n12;
select max(ds) from TEST1_n12;

select distinct ds from srcpart;
select min(ds),max(ds) from srcpart;

-- HIVE-3594 URI encoding for temporary path
alter table TEST2_n8 add partition (ds='01:10:10', hr='01');
alter table TEST2_n8 add partition (ds='01:10:20', hr='02');

explain extended select ds, count(distinct hr) from TEST2_n8 group by ds;
select ds, count(distinct hr) from TEST2_n8 group by ds;
