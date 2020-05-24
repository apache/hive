--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=false;

create table common_join_table (id  string,
                        col1 string,
                        date_created  date,
                        col2  string,
                        col3  string,
                        time_stamp  timestamp,
                        col4  date,
                         col4key bigint,
                        col5  date,
                        col6  string,
                        col7  string,
                        col8 smallint);

insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);
insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);
insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);
insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);
insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);
insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);
insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);
insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);
insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);
insert into common_join_table values ('id',  '109515', null, 'test', 'test', '2018-01-10 15:03:55.0', '2018-01-10', 109515, null, '45045501', 'id', null);

WITH temp_tbl_1 AS (
SELECT col7
	  ,col4KEY
	  ,COUNT(*) AS temp_result_1
  FROM common_join_table
  GROUP BY col7, col4KEY
),

temp_tbl_2 AS (
SELECT col7
	  ,col4KEY
	  ,temp_result_1
	  ,ROW_NUMBER() OVER(PARTITION BY col7 ORDER BY col4KEY ASC) AS temp_result_2
  FROM temp_tbl_1
),

temp_tbl_3 AS (
SELECT col7
	  ,MIN(col4KEY) AS START_DATE
	  ,MAX(col4KEY) AS END_DATE
  FROM temp_tbl_2
  GROUP BY col7
),


temp_tbl_4 AS (
SELECT D1.col7
	  ,D1.col4KEY
	  ,D1.temp_result_2
	  ,D1.temp_result_1
	  ,CASE WHEN D2.col4KEY-D1.col4KEY > 30 THEN D1.col4KEY
	  	WHEN D1.col4KEY = M.END_DATE THEN D1.col4KEY ELSE 0 END AS temp_result_3
	  ,CASE WHEN D2.col4KEY-D1.col4KEY > 30 THEN D2.col4KEY
	  	WHEN D1.col4KEY = M.START_DATE THEN D1.col4KEY ELSE 0 END AS temp_result_4
  FROM temp_tbl_2 D1
  INNER JOIN temp_tbl_3 M
  ON D1.col7 = M.col7
  LEFT JOIN temp_tbl_2 D2
  ON D1.col7 = D2.col7
  AND D1.temp_result_2 = D2.temp_result_2+1
),

temp_tbl_5 AS (
SELECT S1.col7
	  ,S1.col4KEY
	  ,S1.temp_result_2
	  ,S1.temp_result_1
	  ,CASE WHEN S1.col4KEY >= S2.temp_result_4
	  	AND S1.col4KEY <= S3.temp_result_3
		THEN 1 ELSE 0 END AS temp_result_5
  FROM temp_tbl_4 S1
  LEFT JOIN temp_tbl_4 S2
  ON S1.col7 = S2.col7
  AND S2.temp_result_4 != 0
  LEFT JOIN temp_tbl_4 S3
  ON S1.col7 = S3.col7
  AND S3.temp_result_3 != 0
),

temp_tbl_6 AS (
SELECT col7
	  ,col4KEY
	  ,temp_result_2
	  ,temp_result_1
	  ,SUM(temp_result_5) AS temp_result_5
  FROM temp_tbl_5
  GROUP BY col7
	  ,col4KEY
	  ,temp_result_2
	  ,temp_result_1
),

temp_tbl_7 AS (
SELECT col7
	  ,SUM(temp_result_2) AS temp_result_6
	  ,SUM(temp_result_1) AS temp_result_1
  FROM temp_tbl_6
  GROUP BY col7
)

SELECT S.*
  FROM temp_tbl_6 S
  INNER JOIN
  temp_tbl_7 F
  ON S.col7 = F.col7
  --WHERE F.temp_result_6 < 40
  --AND F.temp_result_1 < 200
;

drop table common_join_table;

