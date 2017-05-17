-- Check PTFOperator is able to reset PTFPersistence (https://issues.apache.org/jira/browse/HIVE-4932)
DROP TABLE ptf_persistence_table;
CREATE TABLE ptf_persistence_table(
  key int,
  value string) 
STORED AS TEXTFILE
LOCATION '${hiveconf:test.blobstore.path.unique}/ptf_persistence/ptf_persistence_table';

LOAD DATA LOCAL INPATH '../../data/files/srcbucket1.txt' INTO TABLE ptf_persistence_table;

SET hive.conf.validation=false;
SET hive.ptf.partition.persistence.memsize=32;
SELECT key, value, NTILE(10)
OVER (PARTITION BY value ORDER BY key DESC)
FROM ptf_persistence_table;
