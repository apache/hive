set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

CREATE TABLE tab1_n0(key1 int, value string) CLUSTERED BY (key1) INTO 10 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab2_n0 (key1 int, value string) CLUSTERED BY (key1) INTO 10 BUCKETS STORED AS TEXTFILE;


-- HIVE-18721 : Make sure only certain buckets have data.
insert into tab1_n0 VALUES (1,"abc"),(4,"def"),(8, "ghi");
insert into tab2_n0 VALUES (1, "abc"), (5, "aa");

set hive.convert.join.bucket.mapjoin.tez = true;

explain select * from tab1_n0, tab2_n0 where tab1_n0.key1 = tab2_n0.key1;
select * from tab1_n0, tab2_n0 where tab1_n0.key1 = tab2_n0.key1;
