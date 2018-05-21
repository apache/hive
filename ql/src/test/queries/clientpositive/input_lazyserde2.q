--! qt:dataset:srcbucket2
--! qt:dataset:src_thrift
-- Suppress vectorization due to known bug.  See HIVE-19088.
set hive.vectorized.execution.enabled=false;
set hive.test.vectorized.execution.enabled.override=none;

-- SORT_QUERY_RESULTS

DROP TABLE dest1_n148;
CREATE TABLE dest1_n148(a array<int>, b array<string>, c map<string,string>, d int, e string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '1'
COLLECTION ITEMS TERMINATED BY '2'
MAP KEYS TERMINATED BY '3'
LINES TERMINATED BY '10'
STORED AS TEXTFILE;

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE dest1_n148 SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring DISTRIBUTE BY 1;

FROM src_thrift
INSERT OVERWRITE TABLE dest1_n148 SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring DISTRIBUTE BY 1;

SELECT dest1_n148.* FROM dest1_n148 CLUSTER BY 1;

SELECT dest1_n148.a[0], dest1_n148.b[0], dest1_n148.c['key2'], dest1_n148.d, dest1_n148.e FROM dest1_n148 CLUSTER BY 1;

DROP TABLE dest1_n148;

CREATE TABLE dest1_n148(a array<int>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '1' ESCAPED BY '\\';
INSERT OVERWRITE TABLE dest1_n148 SELECT src_thrift.lint FROM src_thrift DISTRIBUTE BY 1;
SELECT * from dest1_n148;
DROP TABLE dest1_n148;

CREATE TABLE dest1_n148(a map<string,string>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '1' ESCAPED BY '\\';
INSERT OVERWRITE TABLE dest1_n148 SELECT src_thrift.mstringstring FROM src_thrift DISTRIBUTE BY 1;
SELECT * from dest1_n148;

CREATE TABLE destBin_n0(a UNIONTYPE<int, double, array<string>, struct<col1:int,col2:string>>) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2' STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE destBin_n0 SELECT create_union( CASE WHEN key < 100 THEN 0 WHEN key < 200 THEN 1 WHEN key < 300 THEN 2 WHEN key < 400 THEN 3 ELSE 0 END, key, 2.0D, array("one","two"), struct(5,"five")) FROM srcbucket2;
SELECT * from destBin_n0;
DROP TABLE destBin_n0;

DROP TABLE dest2_n38;
DROP TABLE dest3_n6;

CREATE TABLE dest2_n38 (a map<string,map<string,map<string,uniontype<int, bigint, string, double, boolean, array<string>, map<string,string>>>>>)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2' STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE dest2_n38 SELECT src_thrift.attributes FROM src_thrift;
SELECT a from dest2_n38 limit 10;

CREATE TABLE dest3_n6 (
unionfield1 uniontype<int, bigint, string, double, boolean, array<string>, map<string,string>>,
unionfield2 uniontype<int, bigint, string, double, boolean, array<string>, map<string,string>>,
unionfield3 uniontype<int, bigint, string, double, boolean, array<string>, map<string,string>>
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2' STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE dest3_n6 SELECT src_thrift.unionField1,src_thrift.unionField2,src_thrift.unionField3 from src_thrift;
SELECT unionfield1, unionField2, unionfield3 from dest3_n6 limit 10;
