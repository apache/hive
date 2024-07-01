--! qt:dataset:src_thrift
set hive.cbo.fallback.strategy=NEVER;
CREATE TABLE DEST1_n129(Key INT, VALUE STRING) STORED AS TEXTFILE;

EXPLAIN
FROM SRC_THRIFT
INSERT OVERWRITE TABLE dest1_n129 SELECT src_Thrift.LINT[1], src_thrift.lintstring[0].MYSTRING where src_thrift.liNT[0] > 0;

FROM SRC_THRIFT
INSERT OVERWRITE TABLE dest1_n129 SELECT src_Thrift.LINT[1], src_thrift.lintstring[0].MYSTRING where src_thrift.liNT[0] > 0;

SELECT DEST1_n129.* FROM Dest1_n129;
