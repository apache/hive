--! qt:dataset:src_thrift
CREATE TABLE dest1_n6(key INT, value STRING, mapvalue STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE dest1_n6 SELECT src_thrift.lint[1], src_thrift.lintstring[0].mystring, src_thrift.mstringstring['key_2'];

FROM src_thrift
INSERT OVERWRITE TABLE dest1_n6 SELECT src_thrift.lint[1], src_thrift.lintstring[0].mystring, src_thrift.mstringstring['key_2'];

SELECT dest1_n6.* FROM dest1_n6;
