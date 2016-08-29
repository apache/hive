set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;

drop table simple_mm;
drop table partunion_mm;
drop table merge_mm;
drop table ctas_mm;
drop table T1;
drop table T2;
drop table skew_mm;


create table simple_mm(key int) partitioned by (key_mm int)  tblproperties ('hivecommit'='true');
insert into table simple_mm partition(key_mm='455') select key from src limit 3;

create table ctas_mm tblproperties ('hivecommit'='true') as select * from src limit 3;

create table partunion_mm(id_mm int) partitioned by (key_mm int)  tblproperties ('hivecommit'='true');


insert into table partunion_mm partition(key_mm)
select temps.* from (
select key as key_mm, key from ctas_mm 
union all 
select key as key_mm, key from simple_mm ) temps;

set hive.merge.mapredfiles=true;
set hive.merge.sparkfiles=true;
set hive.merge.tezfiles=true;

CREATE TABLE merge_mm (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC tblproperties ('hivecommit'='true');

EXPLAIN
INSERT OVERWRITE TABLE merge_mm PARTITION (ds='123', part)
        SELECT key, value, PMOD(HASH(key), 2) as part
        FROM src;

INSERT OVERWRITE TABLE merge_mm PARTITION (ds='123', part)
        SELECT key, value, PMOD(HASH(key), 2) as part
        FROM src;


set hive.optimize.skewjoin.compiletime = true;
-- the test case is wrong?

CREATE TABLE T1(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;
CREATE TABLE T2(key STRING, val STRING)
SKEWED BY (key) ON ((3)) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;

EXPLAIN
SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key;

create table skew_mm(k1 string, k2 string, k3 string, k4 string) SKEWED BY (key) ON ((2)) tblproperties ('hivecommit'='true');
INSERT OVERWRITE TABLE skew_mm
SELECT a.key as k1, a.val as k2, b.key as k3, b.val as k4 FROM T1 a JOIN T2 b ON a.key = b.key;

-- TODO load, acid, etc
