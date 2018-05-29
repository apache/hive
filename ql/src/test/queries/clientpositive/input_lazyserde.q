--! qt:dataset:srcbucket2
--! qt:dataset:src_thrift
-- Suppress vectorization due to known bug.  See HIVE-19088.
set hive.vectorized.execution.enabled=false;
set hive.test.vectorized.execution.enabled.override=none;

-- SORT_QUERY_RESULTS

DROP TABLE dest1_n43;
CREATE TABLE dest1_n43(a array<int>, b array<string>, c map<string,string>, d int, e string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '1'
COLLECTION ITEMS TERMINATED BY '2'
MAP KEYS TERMINATED BY '3'
LINES TERMINATED BY '10'
STORED AS TEXTFILE;

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE dest1_n43 SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring DISTRIBUTE BY 1;

FROM src_thrift
INSERT OVERWRITE TABLE dest1_n43 SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring DISTRIBUTE BY 1;

SELECT dest1_n43.* FROM dest1_n43 CLUSTER BY 1;

SELECT dest1_n43.a[0], dest1_n43.b[0], dest1_n43.c['key2'], dest1_n43.d, dest1_n43.e FROM dest1_n43 CLUSTER BY 1;

DROP TABLE dest1_n43;

CREATE TABLE dest1_n43(a array<int>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '1' ESCAPED BY '\\';
INSERT OVERWRITE TABLE dest1_n43 SELECT src_thrift.lint FROM src_thrift DISTRIBUTE BY 1;
SELECT * from dest1_n43;
DROP TABLE dest1_n43;

CREATE TABLE dest1_n43(a map<string,string>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '1' ESCAPED BY '\\';
INSERT OVERWRITE TABLE dest1_n43 SELECT src_thrift.mstringstring FROM src_thrift DISTRIBUTE BY 1;
SELECT * from dest1_n43;

CREATE TABLE destBin(a UNIONTYPE<int, double, array<string>, struct<col1:int,col2:string>>) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe' STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE destBin SELECT create_union( CASE WHEN key < 100 THEN 0 WHEN key < 200 THEN 1 WHEN key < 300 THEN 2 WHEN key < 400 THEN 3 ELSE 0 END, key, 2.0D, array("one","two"), struct(5,"five")) FROM srcbucket2;
SELECT * from destBin;
DROP TABLE destBin;

DROP TABLE dest2_n7;
DROP TABLE dest3;

CREATE TABLE dest2_n7 (a map<string,map<string,map<string,uniontype<int, bigint, string, double, boolean, array<string>, map<string,string>>>>>)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe' STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE dest2_n7 SELECT src_thrift.attributes FROM src_thrift;
SELECT a from dest2_n7 limit 10;

CREATE TABLE dest3 (
unionfield1 uniontype<int, bigint, string, double, boolean, array<string>, map<string,string>>,
unionfield2 uniontype<int, bigint, string, double, boolean, array<string>, map<string,string>>,
unionfield3 uniontype<int, bigint, string, double, boolean, array<string>, map<string,string>>
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe' STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE dest3 SELECT src_thrift.unionField1,src_thrift.unionField2,src_thrift.unionField3 from src_thrift;
SELECT unionfield1, unionField2, unionfield3 from dest3 limit 10;
