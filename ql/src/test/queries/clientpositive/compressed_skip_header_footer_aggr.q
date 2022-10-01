SET hive.query.results.cache.enabled=false;
SET hive.mapred.mode=nonstrict;
SET hive.explain.user=false;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase1;
dfs -copyFromLocal ../../data/files/compressed_4line_file1.csv  ${system:test.tmp.dir}/testcase1/;

CREATE EXTERNAL TABLE `testcase1`(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  LOCATION '${system:test.tmp.dir}/testcase1'
  TBLPROPERTIES ("skip.header.line.count"="1", "skip.footer.line.count"="1");

CREATE EXTERNAL TABLE `testcase2`(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  LOCATION '${system:test.tmp.dir}/testcase1'
  TBLPROPERTIES ("skip.header.line.count"="1", "skip.footer.line.count"="0");

CREATE EXTERNAL TABLE `testcase3`(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  LOCATION '${system:test.tmp.dir}/testcase1'
  TBLPROPERTIES ("skip.header.line.count"="0", "skip.footer.line.count"="1");

SET hive.fetch.task.conversion = more;

select * from testcase1;
select count(*) from testcase1;

select * from testcase2;
select count(*) from testcase2;

select * from testcase3;
select count(*) from testcase3;

SET hive.fetch.task.conversion = none;


select * from testcase1;
select count(*) from testcase1;

select * from testcase2;
select count(*) from testcase2;

select * from testcase3;
select count(*) from testcase3;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase2;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase3;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase4;
dfs -copyFromLocal ../../data/files/compressed_4line_file2.csv.bz2  ${system:test.tmp.dir}/testcase2/;
dfs -copyFromLocal ../../data/files/compressed_4line_file2.csv.bz2  ${system:test.tmp.dir}/testcase3/;
dfs -copyFromLocal ../../data/files/compressed_4line_file2.csv.bz2  ${system:test.tmp.dir}/testcase4/;
--
-- Stored encoded in Cache so need to create separate Tables
CREATE EXTERNAL TABLE `testcase4`(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  LOCATION '${system:test.tmp.dir}/testcase2'
  TBLPROPERTIES ("skip.header.line.count"="1", "skip.footer.line.count"="1");

CREATE EXTERNAL TABLE `testcase5`(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  LOCATION '${system:test.tmp.dir}/testcase3'
  TBLPROPERTIES ("skip.header.line.count"="1", "skip.footer.line.count"="0");

CREATE EXTERNAL TABLE `testcase6`(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  LOCATION '${system:test.tmp.dir}/testcase4'
  TBLPROPERTIES ("skip.header.line.count"="0", "skip.footer.line.count"="1");

SET hive.fetch.task.conversion = more;

select * from testcase4;
select count(*) from testcase4;

select * from testcase5;
select count(*) from testcase5;

select * from testcase6;
select count(*) from testcase6;

SET hive.fetch.task.conversion = none;

select * from testcase4;
select count(*) from testcase4;

select * from testcase5;
select count(*) from testcase5;

select * from testcase6;
select count(*) from testcase6;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase_gz;
dfs -copyFromLocal ../../data/files/test.csv.gz  ${system:test.tmp.dir}/testcase_gz/;

create table testcase_gz(age int, name string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
stored as textfile LOCATION '${system:test.tmp.dir}/testcase_gz'
TBLPROPERTIES ("skip.header.line.count"="1", "skip.footer.line.count"="1");

SET hive.fetch.task.conversion = more;
select * from testcase_gz;
select count(*) from testcase_gz;

set hive.fetch.task.conversion=none;
select * from testcase_gz;
select count(*) from testcase_gz;

-- clean up testdata
dfs -rmr ${system:test.tmp.dir}/testcase_gz;
dfs -rmr ${system:test.tmp.dir}/testcase1/;
dfs -rmr ${system:test.tmp.dir}/testcase2/;
dfs -rmr ${system:test.tmp.dir}/testcase3/;
dfs -rmr ${system:test.tmp.dir}/testcase4/;
