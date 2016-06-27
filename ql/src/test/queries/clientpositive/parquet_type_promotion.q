DROP TABLE parquet_type_promotion_staging;
DROP TABLE parquet_type_promotion;

SET hive.metastore.disallow.incompatible.col.type.changes=false;

CREATE TABLE parquet_type_promotion_staging (
  cint int,
  cint2 int,
  cint3 int,
  clong bigint,
  clong2 bigint,
  cfloat float,
  cdouble double,
  m1 map<string, int>,
  m2 map<string, bigint>,
  l1 array<int>,
  l2 array<bigint>,
  st1 struct<c1:int, c2:int>,
  fm1 map<string, float>,
  fl1 array<float>,
  fst1 struct<c1:float, c2:float>
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH '../../data/files/parquet_type_promotion.txt' OVERWRITE INTO TABLE parquet_type_promotion_staging;

SELECT * FROM parquet_type_promotion_staging;

CREATE TABLE parquet_type_promotion (
  cint int,
  cint2 int,
  cint3 int,
  clong bigint,
  clong2 bigint,
  cfloat float,
  cdouble double,
  m1 map<string, int>,
  m2 map<string, bigint>,
  l1 array<int>,
  l2 array<bigint>,
  st1 struct<c1:int, c2:int>,
  fm1 map<string, float>,
  fl1 array<float>,
  fst1 struct<c1:float, c2:float>
) STORED AS PARQUET;

INSERT OVERWRITE TABLE parquet_type_promotion
   SELECT * FROM parquet_type_promotion_staging;

SELECT * FROM parquet_type_promotion;

ALTER TABLE  parquet_type_promotion REPLACE COLUMNS(
  cint bigint,
  cint2 float,
  cint3 double,
  clong float,
  clong2 double,
  cfloat double,
  cdouble double,
  m1 map<string, bigint>,
  m2 map<string, float>,
  l1 array<bigint>,
  l2 array<double>,
  st1 struct<c1:int, c2:bigint>,
  fm1 map<string, double>,
  fl1 array<double>,
  fst1 struct<c1:double, c2:float>
);

SELECT * FROM parquet_type_promotion;

-- This test covers the case where array<struct<f1,f2,..>> data
-- can be retrieved useing map<f1,f2>.
-- This also test if there are more than 2 fields in array_of_struct
-- which can be read as  map as well.

DROP TABLE arrays_of_struct_to_map;
CREATE TABLE arrays_of_struct_to_map (locations1 array<struct<c1:int,c2:int>>, locations2 array<struct<f1:int,
f2:int,f3:int>>) STORED AS PARQUET;
INSERT INTO TABLE arrays_of_struct_to_map select array(named_struct("c1",1,"c2",2)), array(named_struct("f1",
77,"f2",88,"f3",99)) FROM parquet_type_promotion LIMIT 1;
SELECT * FROM arrays_of_struct_to_map;
-- Testing schema evolution of dropping column from array<struct<>>
ALTER TABLE arrays_of_struct_to_map REPLACE COLUMNS (locations1 array<struct<c1:int>>, locations2
array<struct<f2:int>>);
SELECT * FROM arrays_of_struct_to_map;
-- Testing schema evolution of adding columns into array<struct<>>
ALTER TABLE arrays_of_struct_to_map REPLACE COLUMNS (locations1 array<struct<c1:int,c2:int,c3:int>>, locations2
array<struct<f1:int,f2:int,f3:int>>);
SELECT * FROM arrays_of_struct_to_map;
ALTER TABLE arrays_of_struct_to_map REPLACE COLUMNS (locations1 map<int,bigint>, locations2 map<bigint,int>);
SELECT * FROM arrays_of_struct_to_map;
