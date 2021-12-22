SET hive.query.results.cache.enabled=false;
SET hive.mapred.mode=nonstrict;
SET hive.explain.user=false;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase1;
dfs -rmr ${system:test.tmp.dir}/testcase1;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase1;
dfs -copyFromLocal ../../data/files/emptyhead_4line_file1.csv  ${system:test.tmp.dir}/testcase1/;
--
--
DROP TABLE IF EXISTS `testcase1`;
CREATE EXTERNAL TABLE `testcase1`(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  LOCATION '${system:test.tmp.dir}/testcase1'
  TBLPROPERTIES ("skip.header.line.count"="1");

-- Make sure empty Head lines are skipped
-- With Fetch task optimization
SET hive.fetch.task.conversion = more;
select * from testcase1;
select count(*) from testcase1;

-- Make sure empty Head lines are skipped
-- And NO Fetch task optimization
set hive.fetch.task.conversion=none;
select * from testcase1;
select count(*) from testcase1;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase2;
dfs -rmr ${system:test.tmp.dir}/testcase2;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase2;
dfs -copyFromLocal ../../data/files/emptyhead_4line_file1.csv.bz2  ${system:test.tmp.dir}/testcase2/;
--
--
DROP TABLE IF EXISTS `testcase2`;
CREATE EXTERNAL TABLE `testcase2`(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
 LOCATION '${system:test.tmp.dir}/testcase2'
 TBLPROPERTIES ("skip.header.line.count"="1");

SET hive.fetch.task.conversion = more;
select * from testcase2;
select count(*) from testcase2;

set hive.fetch.task.conversion=none;
select * from testcase2;
select count(*) from testcase2;