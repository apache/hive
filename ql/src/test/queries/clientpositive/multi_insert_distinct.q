--! qt:dataset:src

CREATE TABLE tmp1 ( v1 string , v2 string , v3 string ) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
;

INSERT INTO tmp1 VALUES ('v1', 'v2', 'v3'), ('v1', 'v2', 'v3a');


CREATE TABLE tmp_grouped_by_all_col ( v1 string , v2 string , v3 string ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' ;
CREATE TABLE tmp_grouped_by_one_col  ( v1 string , cnt__v2 int , cnt__v3 int ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' ;
CREATE TABLE tmp_grouped_by_two_col  ( v1 string , v2 string , cnt__v3 int ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' ;


set hive.explain.user=false;
set hive.stats.autogather=false; 
set hive.stats.column.autogather=false; 

explain FROM tmp1
INSERT INTO tmp_grouped_by_one_col 
SELECT v1, count(distinct v2), count(distinct v3) GROUP BY v1
INSERT INTO tmp_grouped_by_two_col
SELECT v1, v2, count(distinct v3) GROUP BY v1, v2;

FROM tmp1
INSERT INTO tmp_grouped_by_one_col 
SELECT v1, count(distinct v2), count(distinct v3) GROUP BY v1
INSERT INTO tmp_grouped_by_two_col
SELECT v1, v2, count(distinct v3) GROUP BY v1, v2;

select * from tmp_grouped_by_two_col;

truncate table tmp_grouped_by_two_col;

explain FROM tmp1
INSERT INTO tmp_grouped_by_one_col 
SELECT v1, count(distinct v2), count(distinct v3) GROUP BY v1
INSERT INTO tmp_grouped_by_two_col
SELECT v1, v2, count(v3) GROUP BY v1, v2;

FROM tmp1
INSERT INTO tmp_grouped_by_one_col 
SELECT v1, count(distinct v2), count(distinct v3) GROUP BY v1
INSERT INTO tmp_grouped_by_two_col
SELECT v1, v2, count(v3) GROUP BY v1, v2;

select * from tmp_grouped_by_two_col;

explain FROM tmp1
INSERT INTO tmp_grouped_by_one_col 
SELECT v1, count(distinct v2), count(distinct v3) GROUP BY v1
INSERT INTO tmp_grouped_by_all_col
SELECT v1, v2, v3 GROUP BY v1, v2, v3;

FROM tmp1
INSERT INTO tmp_grouped_by_one_col 
SELECT v1, count(distinct v2), count(distinct v3) GROUP BY v1
INSERT INTO tmp_grouped_by_all_col
SELECT v1, v2, v3 GROUP BY v1, v2, v3;

select * from tmp_grouped_by_all_col;