-- Test MAPJOIN hint

-- SORT_QUERY_RESULTS

DROP TABLE keyval1;
DROP TABLE keyval2;

CREATE TABLE keyval1 (
    key int,
    value string)
STORED AS TEXTFILE
LOCATION '${hiveconf:test.blobstore.path.unique}/map_join/keyval1';

LOAD DATA LOCAL INPATH '../../data/files/srcbucket1.txt' INTO TABLE keyval1;

CREATE TABLE keyval2 (
    key int,
    value string)
STORED AS TEXTFILE
LOCATION '${hiveconf:test.blobstore.path.unique}/map_join/keyval2';

LOAD DATA LOCAL INPATH '../../data/files/srcbucket0.txt' INTO TABLE keyval2;

SELECT /*+ MAPJOIN(x) */ x.key, x.value, y.value
FROM keyval1 x
JOIN keyval2 y ON (x.value = y.value);