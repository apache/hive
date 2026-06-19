--! qt:dataset:src_thrift
CREATE TABLE dest1_n32(lint_size INT, lintstring_size INT, mstringstring_size INT) STORED AS TEXTFILE;

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE dest1_n32 SELECT size(src_thrift.lint), size(src_thrift.lintstring), size(src_thrift.mstringstring) where src_thrift.lint IS NOT NULL AND NOT (src_thrift.mstringstring IS NULL);

FROM src_thrift
INSERT OVERWRITE TABLE dest1_n32 SELECT size(src_thrift.lint), size(src_thrift.lintstring), size(src_thrift.mstringstring) where src_thrift.lint IS NOT NULL AND NOT (src_thrift.mstringstring IS NULL);

SELECT dest1_n32.* FROM dest1_n32;
