set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

drop table sales;
drop table things;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE sales (name STRING, id INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE things (id INT, name STRING) partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

load data local inpath '../../data/files/sales.txt' INTO TABLE sales;
load data local inpath '../../data/files/things.txt' INTO TABLE things partition(ds='2011-10-23');
load data local inpath '../../data/files/things2.txt' INTO TABLE things partition(ds='2011-10-24');

SELECT name,id FROM sales;

SELECT id,name FROM things;

SELECT name,id FROM sales LEFT SEMI JOIN things ON (sales.id = things.id);

drop table sales;
drop table things;

-- HIVE-15458
explain select part.p_type from part join (select p1.p_name from part p1, part p2 group by p1.p_name) pp ON pp.p_name = part.p_name;
select part.p_type from part join (select p1.p_name from part p1, part p2 group by p1.p_name) pp ON pp.p_name = part.p_name;

-- Semi join optmization should take out the right side
explain select part.p_type from part left join (select p1.p_name from part p1, part p2 group by p1.p_name) pp ON pp.p_name = part.p_name;
select part.p_type from part left join (select p1.p_name from part p1, part p2 group by p1.p_name) pp ON pp.p_name = part.p_name;
