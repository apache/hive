--! qt:dataset:src
--! qt:dataset:part


set hive.exec.reducers.max = 1;

CREATE TABLE srcbucket_mapjoin_part_1_n3 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) SORTED BY (key DESC) INTO 2 BUCKETS;
INSERT OVERWRITE TABLE srcbucket_mapjoin_part_1_n3 PARTITION (part='1') SELECT * FROM src;

CREATE TABLE srcbucket_mapjoin_part_2_n8 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) SORTED BY (value DESC) INTO 2 BUCKETS;
INSERT OVERWRITE TABLE srcbucket_mapjoin_part_2_n8 PARTITION (part='1') SELECT * FROM src;

ALTER TABLE srcbucket_mapjoin_part_2_n8 CLUSTERED BY (key) SORTED BY (key DESC) INTO 2 BUCKETS;

set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.cbo.enable=false;
-- The table sorting metadata matches but the partition metadata does not, sorted merge join should not be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n3 a JOIN srcbucket_mapjoin_part_2_n8 b
ON a.key = b.key AND a.part = '1' AND b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n3 a JOIN srcbucket_mapjoin_part_2_n8 b
ON a.key = b.key AND a.part = '1' AND b.part = '1';
