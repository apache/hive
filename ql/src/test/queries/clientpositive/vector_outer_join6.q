set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

create table TJOIN1_txt (RNUM int , C1 int, C2 int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

create table TJOIN2_txt (RNUM int , C1 int, C2 char(2))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

create table if not exists TJOIN3_txt (RNUM int , C1 int, C2 char(2))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

create table TJOIN4_txt (RNUM int , C1 int, C2 char(2))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

load data local inpath '../../data/files/TJOIN1' into table TJOIN1_txt;
load data local inpath '../../data/files/TJOIN2' into table TJOIN2_txt;
load data local inpath '../../data/files/TJOIN3' into table TJOIN3_txt;
load data local inpath '../../data/files/TJOIN4' into table TJOIN4_txt;

create table TJOIN1 stored as orc AS SELECT * FROM TJOIN1_txt;
create table TJOIN2 stored as orc AS SELECT * FROM TJOIN2_txt;
create table TJOIN3 stored as orc AS SELECT * FROM TJOIN3_txt;
create table TJOIN4 stored as orc AS SELECT * FROM TJOIN4_txt;

explain vectorization detail formatted
select tj1rnum, tj2rnum, tjoin3.rnum as rnumt3 from
   (select tjoin1.rnum tj1rnum, tjoin2.rnum tj2rnum, tjoin2.c1 tj2c1 from tjoin1 left outer join tjoin2 on tjoin1.c1 = tjoin2.c1 ) tj left outer join tjoin3 on tj2c1 = tjoin3.c1;

select tj1rnum, tj2rnum, tjoin3.rnum as rnumt3 from
   (select tjoin1.rnum tj1rnum, tjoin2.rnum tj2rnum, tjoin2.c1 tj2c1 from tjoin1 left outer join tjoin2 on tjoin1.c1 = tjoin2.c1 ) tj left outer join tjoin3 on tj2c1 = tjoin3.c1;

explain vectorization detail formatted
select tj1rnum, tj2rnum as rnumt3 from
   (select tjoin1.rnum tj1rnum, tjoin2.rnum tj2rnum, tjoin2.c1 tj2c1 from tjoin1 left outer join tjoin2 on tjoin1.c1 = tjoin2.c1 ) tj left outer join tjoin3 on tj2c1 = tjoin3.c1;

select tj1rnum, tj2rnum as rnumt3 from
   (select tjoin1.rnum tj1rnum, tjoin2.rnum tj2rnum, tjoin2.c1 tj2c1 from tjoin1 left outer join tjoin2 on tjoin1.c1 = tjoin2.c1 ) tj left outer join tjoin3 on tj2c1 = tjoin3.c1;
