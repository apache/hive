-- Test MAPJOIN with filters in the ON condition
set hive.merge.nway.joins=true;
DROP TABLE src_a_data;
CREATE TABLE src_a_data (
    key int,
    value string)
STORED AS TEXTFILE
LOCATION '${hiveconf:test.blobstore.path.unique}/map_join_on_filter/src_a_data';

LOAD DATA LOCAL INPATH '../../data/files/smbbucket_1.txt' INTO TABLE src_a_data;

SELECT /*+ MAPJOIN(src1, src2) */ *
FROM src_a_data src1
RIGHT OUTER JOIN src_a_data src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
JOIN src_a_data src3 ON (src2.key = src3.key AND src3.key < 10)
SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;
