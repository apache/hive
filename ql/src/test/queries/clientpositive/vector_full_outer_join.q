set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- SORT_QUERY_RESULTS

drop table if exists TJOIN1;
drop table if exists TJOIN2;
create table if not exists TJOIN1 (RNUM int , C1 int, C2 int) STORED AS orc;
create table if not exists TJOIN2 (RNUM int , C1 int, C2 char(2)) STORED AS orc;
create table if not exists TJOIN1STAGE (RNUM int , C1 int, C2 char(2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE ;
create table if not exists TJOIN2STAGE (RNUM int , C1 int, C2 char(2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE ;
LOAD DATA LOCAL INPATH '../../data/files/tjoin1.txt' OVERWRITE INTO TABLE TJOIN1STAGE;
LOAD DATA LOCAL INPATH '../../data/files/tjoin2.txt' OVERWRITE INTO TABLE TJOIN2STAGE;
INSERT INTO TABLE TJOIN1 SELECT * from TJOIN1STAGE;
INSERT INTO TABLE TJOIN2 SELECT * from TJOIN2STAGE;

SET hive.mapjoin.full.outer=true;

set hive.vectorized.execution.enabled=false;
set hive.mapjoin.hybridgrace.hashtable=false;
explain vectorization detail
select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

set hive.vectorized.execution.enabled=false;
set hive.mapjoin.hybridgrace.hashtable=true;
explain vectorization detail
select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );


set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=false;
SET hive.vectorized.execution.mapjoin.native.enabled=false;
explain vectorization detail
select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=true;
SET hive.vectorized.execution.mapjoin.native.enabled=false;
explain vectorization detail
select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=false;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
explain vectorization detail
select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
explain vectorization detail
select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c1 as c1j2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );


-- Omit tjoin2.c1
explain vectorization detail
select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

select tjoin1.rnum, tjoin1.c1, tjoin1.c2, tjoin2.c2 as c2j2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

-- Omit tjoin2.c1 and tjoin2.c2
explain vectorization detail
select tjoin1.rnum, tjoin1.c1, tjoin1.c2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );

select tjoin1.rnum, tjoin1.c1, tjoin1.c2 from tjoin1 full outer join tjoin2 on ( tjoin1.c1 = tjoin2.c1 );
