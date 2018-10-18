--! qt:dataset:part
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

drop table sales_n1;
drop table things_n1;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE sales_n1 (name STRING, id INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE things_n1 (id INT, name STRING) partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

load data local inpath '../../data/files/sales.txt' INTO TABLE sales_n1;
load data local inpath '../../data/files/things.txt' INTO TABLE things_n1 partition(ds='2011-10-23');
load data local inpath '../../data/files/things2.txt' INTO TABLE things_n1 partition(ds='2011-10-24');

SELECT name,id FROM sales_n1;

SELECT id,name FROM things_n1;

SELECT name,id FROM sales_n1 LEFT SEMI JOIN things_n1 ON (sales_n1.id = things_n1.id);

drop table sales_n1;
drop table things_n1;

-- HIVE-15458
explain select part.p_type from part join (select p1.p_name from part p1, part p2 group by p1.p_name) pp ON pp.p_name = part.p_name;
select part.p_type from part join (select p1.p_name from part p1, part p2 group by p1.p_name) pp ON pp.p_name = part.p_name;

-- Semi join optmization should take out the right side
explain select part.p_type from part left join (select p1.p_name from part p1, part p2 group by p1.p_name) pp ON pp.p_name = part.p_name;
select part.p_type from part left join (select p1.p_name from part p1, part p2 group by p1.p_name) pp ON pp.p_name = part.p_name;
