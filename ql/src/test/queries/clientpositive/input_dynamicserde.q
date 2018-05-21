--! qt:dataset:src_thrift
CREATE TABLE dest1_n114(a array<int>, b array<string>, c map<string,string>, d int, e string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '1'
COLLECTION ITEMS TERMINATED BY '2'
MAP KEYS TERMINATED BY '3'
LINES TERMINATED BY '10'
STORED AS TEXTFILE;

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE dest1_n114 SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring;

FROM src_thrift
INSERT OVERWRITE TABLE dest1_n114 SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring;

SELECT dest1_n114.* FROM dest1_n114;

SELECT dest1_n114.a[0], dest1_n114.b[0], dest1_n114.c['key2'], dest1_n114.d, dest1_n114.e FROM dest1_n114;
