-- Test nested outer join queries

DROP TABLE src_a_data;
CREATE TABLE src_a_data (
    key int,
    value string)
STORED AS TEXTFILE
LOCATION '${hiveconf:test.blobstore.path.unique}/nested_outer_join/src_a_data';

LOAD DATA LOCAL INPATH '../../data/files/smbbucket_3.txt' INTO TABLE src_a_data;

SELECT s.keya, s.keyb, c.key keyc
FROM (
    SELECT a.key keya, b.key keyb
    FROM src_a_data a
    LEFT OUTER JOIN src_a_data b ON (a.key=b.key)
) s
LEFT OUTER JOIN src_a_data c ON (s.keyb=c.key AND s.keyb<10)
WHERE s.keya<20;

SELECT a.key keya, b.key keyb, c.key keyc
FROM src_a_data a
LEFT OUTER JOIN src_a_data b ON (a.key=b.key)
LEFT OUTER JOIN src_a_data c ON (b.key=c.key AND b.key<10)
WHERE a.key<20;