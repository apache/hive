set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

drop table if exists TJOIN3;
drop table if exists TJOIN4;
create table if not exists TJOIN3 (name string, id int, flag string) STORED AS orc;
create table if not exists TJOIN4 (code_name string, id int) STORED AS orc;
create table if not exists TJOIN3STAGE (name string, id int, flag string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE ;
create table if not exists TJOIN4STAGE (code_name string, id int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE ;
LOAD DATA LOCAL INPATH '../../data/files/tjoin3.txt' OVERWRITE INTO TABLE TJOIN3STAGE;
LOAD DATA LOCAL INPATH '../../data/files/tjoin4.txt' OVERWRITE INTO TABLE TJOIN4STAGE;
INSERT INTO TABLE TJOIN3 SELECT * from TJOIN3STAGE;
INSERT INTO TABLE TJOIN4 SELECT * from TJOIN4STAGE;

set hive.vectorized.execution.enabled=false;
explain vectorization detail
select TJOIN3.id,TJOIN3.name from TJOIN3 left outer join ( select code_name, id from TJOIN4) s3 on (TJOIN3.name = s3.code_name and TJOIN3.flag='N');

select TJOIN3.id,TJOIN3.name from TJOIN3 left outer join ( select code_name, id from TJOIN4) s3 on (TJOIN3.name = s3.code_name and TJOIN3.flag='N');

explain vectorization detail
select TJOIN3.id,TJOIN3.name from TJOIN3 left outer join ( select code_name, id from TJOIN4) s3 on (TJOIN3.name = s3.code_name and TJOIN3.flag='N') limit 1;

select TJOIN3.id,TJOIN3.name from TJOIN3 left outer join ( select code_name, id from TJOIN4) s3 on (TJOIN3.name = s3.code_name and TJOIN3.flag='N') limit 1;
