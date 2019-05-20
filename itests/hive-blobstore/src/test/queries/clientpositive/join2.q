-- Test left and right outer join
-- SORT_QUERY_RESULTS

DROP TABLE join_src;
CREATE TABLE join_src (
    key int,
    value string)
STORED AS TEXTFILE
LOCATION '${hiveconf:test.blobstore.path.unique}/join2/join_src';

LOAD DATA LOCAL INPATH '../../data/files/smbbucket_1.txt' INTO TABLE join_src;

SELECT *
FROM join_src a
LEFT OUTER JOIN join_src b ON (a.key=b.key AND a.key < 10);

SELECT *
FROM join_src a
RIGHT OUTER JOIN join_src b ON (a.key=b.key AND a.key < 10);