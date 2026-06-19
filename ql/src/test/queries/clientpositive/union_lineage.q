create database db1;
CREATE EXTERNAL TABLE db1.table1
(
   `level1` string,                                 
   `level1_desc` string,                            
   `level2` string,                                 
   `level2_desc` string,                            
   `level3` string,                                 
   `level3_desc` string,                            
   `level4` string,                                 
   `level4_desc` string,                            
   `level5` string,                                 
   `level5_desc` string,                            
   `level6` string,                                 
   `level6_desc` string,                            
   `level7` string,                                 
   `level7_desc` string,                            
   `level8` string,                                 
   `level8_desc` string,                            
   `level9` string,                                 
   `level9_desc` string,                            
   `level10` string,                                
   `level10_desc` string,                           
   `level11` string,                                
   `level11_desc` string,                           
   `level12` string,                                
   `level12_desc` string,                           
   `level13` string,                                
   `level13_desc` string,                           
   `level14` string,                                
   `level14_desc` string,                           
   `level15` string,                                
   `level15_desc` string,                           
   `pack` string,                                   
   aeoi string,
   aeoo string)
 PARTITIONED BY (                                   
   data_date string) stored as parquet;


explain create view if not exists db1.view1 as
SELECT
h.level1,
h.level1_desc,
h.level2,
h.level2_desc,
h.level3,
h.level3_desc,
h.level4,
h.level4_desc,
h.level5,
h.level5_desc,
h.level6,
h.level6_desc,
h.level7,
h.level7_desc,
h.level8,
h.level8_desc,
h.level9,
h.level9_desc,
h.level10,
h.level10_desc,
h.level11,
h.level11_desc,
h.level12,
h.level12_desc,
h.level13,
h.level13_desc,
h.level14,
h.level14_desc,
h.pack,
h.aeoi,
h.aeoo,
h.data_date,
CONCAT('#', h.level1, '#', h.level2, '#', h.level3, '#', h.level4, '#', h.level5, '#', h.level6, '#', h.level7, '#', h.level8, '#', h.level9, '#', h.level10, '#', h.level11, '#', h.level12, '#', h.level13, '#', h.level14, '#') key
FROM
(SELECT MAX(data_date) data_as_of_date, level1 FROM db1.table1 WHERE level1 = 'N00000' GROUP BY level1) d
JOIN db1.table1 h
ON (h.data_date = d.data_as_of_date
AND h.level1 = d.level1)
WHERE
'TM1-5460' NOT IN (h.level1, h.level2, h.level3, h.level4, h.level5, h.level6, h.level7, h.level8, h.level9, h.level10, h.level11, h.level12, h.level13, h.level14, h.level15)
UNION ALL SELECT DISTINCT
hhh.level1,
hhh.level1_desc,
hhh.level2,
hhh.level2_desc,
hhh.level3,
hhh.level3_desc,
hhh.level4,
hhh.level4_desc,
hhh.level5,
hhh.level5_desc,
hhh.level6,
hhh.level6_desc,
hhh.level7,
hhh.level7_desc,
hhh.level8,
hhh.level8_desc,
hhh.level9,
hhh.level9_desc,
hhh.level10,
hhh.level10_desc,
hhh.level11,
hhh.level11_desc,
hhh.level12,
hhh.level12_desc,
hhh.level13,
hhh.level13_desc,
hhh.level14,
hhh.level14_desc,
hhh.pack,
hhh.aeoi,
hhh.aeoo,
hhh.data_date,
CONCAT('#', hhh.level1, '#', hhh.level2, '#', hhh.level3, '#', hhh.level4, '#', hhh.level5, '#', hhh.level6, '#', hhh.level7, '#', hhh.level8, '#', hhh.level9, '#', hhh.level10, '#', hhh.level11, '#', hhh.level12, '#', hhh.level13, '#', hhh.level14, '#') key
FROM
(SELECT
hh.level1,
hh.level1_desc,
CASE WHEN hh.leaf_level < 2 THEN hh.leaf_code ELSE hh.level2 END level2,
CASE WHEN hh.leaf_level < 2 THEN hh.leaf_desc ELSE hh.level2_desc END level2_desc,
CASE WHEN hh.leaf_level < 3 THEN hh.leaf_code ELSE hh.level3 END level3,
CASE WHEN hh.leaf_level < 3 THEN hh.leaf_desc ELSE hh.level3_desc END level3_desc,
CASE WHEN hh.leaf_level < 4 THEN hh.leaf_code ELSE hh.level4 END level4,
CASE WHEN hh.leaf_level < 4 THEN hh.leaf_desc ELSE hh.level4_desc END level4_desc,
CASE WHEN hh.leaf_level < 5 THEN hh.leaf_code ELSE hh.level5 END level5,
CASE WHEN hh.leaf_level < 5 THEN hh.leaf_desc ELSE hh.level5_desc END level5_desc,
CASE WHEN hh.leaf_level < 6 THEN hh.leaf_code ELSE hh.level6 END level6,
CASE WHEN hh.leaf_level < 6 THEN hh.leaf_desc ELSE hh.level6_desc END level6_desc,
CASE WHEN hh.leaf_level < 7 THEN hh.leaf_code ELSE hh.level7 END level7,
CASE WHEN hh.leaf_level < 7 THEN hh.leaf_desc ELSE hh.level7_desc END level7_desc,
CASE WHEN hh.leaf_level < 8 THEN hh.leaf_code ELSE hh.level8 END level8,
CASE WHEN hh.leaf_level < 8 THEN hh.leaf_desc ELSE hh.level8_desc END level8_desc,
CASE WHEN hh.leaf_level < 9 THEN hh.leaf_code ELSE hh.level9 END level9,
CASE WHEN hh.leaf_level < 9 THEN hh.leaf_desc ELSE hh.level9_desc END level9_desc,
CASE WHEN hh.leaf_level < 10 THEN hh.leaf_code ELSE hh.level10 END level10,
CASE WHEN hh.leaf_level < 10 THEN hh.leaf_desc ELSE hh.level10_desc END level10_desc,
CASE WHEN hh.leaf_level < 11 THEN hh.leaf_code ELSE hh.level11 END level11,
CASE WHEN hh.leaf_level < 11 THEN hh.leaf_desc ELSE hh.level11_desc END level11_desc,
CASE WHEN hh.leaf_level < 12 THEN hh.leaf_code ELSE hh.level12 END level12,
CASE WHEN hh.leaf_level < 12 THEN hh.leaf_desc ELSE hh.level12_desc END level12_desc,
CASE WHEN hh.leaf_level < 13 THEN hh.leaf_code ELSE hh.level13 END level13,
CASE WHEN hh.leaf_level < 13 THEN hh.leaf_desc ELSE hh.level13_desc END level13_desc,
CASE WHEN hh.leaf_level < 14 THEN hh.leaf_code ELSE hh.level14 END level14,
CASE WHEN hh.leaf_level < 14 THEN hh.leaf_desc ELSE hh.level14_desc END level14_desc,
hh.pack,
hh.aeoi,
hh.aeoo,
hh.data_date
FROM
(SELECT
h.*,
lc.leaf_level,
lc.leaf_code,
CASE lc.leaf_code
WHEN h.level2 THEN h.level2_desc
WHEN h.level3 THEN h.level3_desc
WHEN h.level4 THEN h.level4_desc
WHEN h.level5 THEN h.level5_desc
WHEN h.level6 THEN h.level6_desc
WHEN h.level7 THEN h.level7_desc
WHEN h.level8 THEN h.level8_desc
WHEN h.level9 THEN h.level9_desc
WHEN h.level10 THEN h.level10_desc
WHEN h.level11 THEN h.level11_desc
WHEN h.level12 THEN h.level12_desc
WHEN h.level13 THEN h.level13_desc
ELSE NULL
END leaf_desc
FROM
(SELECT MAX(data_date) data_as_of_date, level1 FROM db1.table1 WHERE level1 = 'N00000' GROUP BY level1) d
JOIN db1.table1 h
ON (h.data_date = d.data_as_of_date
AND h.level1 = d.level1),
(SELECT
l.leaf_level,
'TM1-5460' leaf_code
FROM
(SELECT 3 leaf_level
UNION ALL SELECT 4 leaf_level
UNION ALL SELECT 5 leaf_level
UNION ALL SELECT 6 leaf_level
UNION ALL SELECT 7 leaf_level
UNION ALL SELECT 8 leaf_level
UNION ALL SELECT 9 leaf_level
UNION ALL SELECT 10 leaf_level
UNION ALL SELECT 11 leaf_level
UNION ALL SELECT 12 leaf_level
UNION ALL SELECT 13 leaf_level) l) lc
WHERE
lc.leaf_code =
CASE
WHEN lc.leaf_level = 2 THEN h.level2
WHEN lc.leaf_level = 3 THEN h.level3
WHEN lc.leaf_level = 4 THEN h.level4
WHEN lc.leaf_level = 5 THEN h.level5
WHEN lc.leaf_level = 6 THEN h.level6
WHEN lc.leaf_level = 7 THEN h.level7
WHEN lc.leaf_level = 8 THEN h.level8
WHEN lc.leaf_level = 9 THEN h.level9
WHEN lc.leaf_level = 10 THEN h.level10
WHEN lc.leaf_level = 11 THEN h.level11
WHEN lc.leaf_level = 12 THEN h.level12
WHEN lc.leaf_level = 13 THEN h.level13
END
AND h.level14 <> lc.leaf_code) hh) hhh;

