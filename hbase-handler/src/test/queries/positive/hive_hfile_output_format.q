set hive.explain.user=false;
set hive.fetch.task.conversion=none;

-- dfs -rmr /tmp/coords_hfiles/o;

CREATE TABLE source(id INT, longitude DOUBLE, latitude DOUBLE);

INSERT INTO TABLE source VALUES (1, 23.54, -54.99);

CREATE TABLE coords_hbase(id INT, x DOUBLE, y DOUBLE)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping' = ':key,o:x,o:y',
  'hbase.table.default.storage.type' = 'binary');

SET hfile.family.path=/tmp/coords_hfiles/o; 
SET hive.hbase.generatehfiles=true;

INSERT OVERWRITE TABLE coords_hbase 
SELECT id, longitude, latitude
FROM source
CLUSTER BY id;

EXPLAIN
SELECT * FROM coords_hbase;

SELECT * FROM coords_hbase;

drop table source;
drop table coords_hbase;
dfs -rmr /tmp/coords_hfiles/o;
