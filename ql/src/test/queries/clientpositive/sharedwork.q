set hive.optimize.shared.work.extended=false;

create table MY_TABLE_0001 (
  col_1 string,
  col_3 timestamp,
  col_7 string,
  col_20 string);

create table MY_TABLE_0001_00 (
  col_1 string,
  col_22 string,
  col_23 int);

create table MY_TABLE_0003 (
  col_24 string,
  col_21 string);

create table MY_TABLE_0001_01 (
  col_1 string,
  col_100 string);


explain extended SELECT
  Table__323.col_7,
  CAST(Table__323.col_3 AS DATE) col_3,
  Table__323.col_20,  
  Table__1232.col_21 col_21_1232,
  Table__323.col_1,
  Table__133.col_22,
  Table__879.col_21 col_21_879
  ,Table__133.col_23
FROM MY_TABLE_0001 Table__323
LEFT OUTER JOIN MY_TABLE_0003 Table__1232 ON (Table__323.col_20=Table__1232.col_24)
LEFT OUTER JOIN MY_TABLE_0001_00 Table__133 ON (Table__323.col_1=Table__133.col_1)
LEFT OUTER JOIN MY_TABLE_0003 Table__879 ON (Table__133.col_22=Table__879.col_24)
LEFT OUTER JOIN MY_TABLE_0001_01 Table__1215 ON (Table__323.col_1=Table__1215.col_1 and Table__1215.col_100 = 210)
WHERE 1=1
AND  (cast(Table__323.col_7 AS DOUBLE) IS NOT NULL OR Table__323.col_7 IS NULL)
AND CAST(Table__323.col_3 AS DATE)  BETWEEN  '2018-07-01'  AND  '2019-01-23'
AND Table__323.col_20  IN  ('part1','part2','part3');
