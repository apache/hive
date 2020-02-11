-- Test table in parquet format with non-standard partition locations in blobstore

DROP TABLE src_events;
CREATE TABLE src_events
(
  log_id      BIGINT,
  `time`        BIGINT,
  uid         BIGINT,
  user_id     BIGINT,
  type        INT,
  event_data  STRING,
  session_id  STRING,
  full_uid    BIGINT,
  run_date    STRING,
  game_id     INT,
  event_name  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${hiveconf:test.blobstore.path.unique}/parquet_nonstd_partitions_loc/src_events/';
LOAD DATA LOCAL INPATH '../../data/files/events.txt' INTO TABLE src_events;

DROP TABLE parquet_events;
CREATE TABLE parquet_events
(
  log_id      BIGINT,
  `time`        BIGINT,
  uid         BIGINT,
  user_id     BIGINT,
  type        INT,
  event_data  STRING,
  session_id  STRING,
  full_uid    BIGINT
)
PARTITIONED BY (run_date STRING, game_id INT, event_name STRING)
STORED AS PARQUET
LOCATION '${hiveconf:test.blobstore.path.unique}/parquet_nonstd_partitions_loc/parquet_events/';

SET hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE parquet_events PARTITION (run_date, game_id, event_name)
SELECT * FROM src_events;
SHOW PARTITIONS parquet_events;
SELECT COUNT(*) FROM parquet_events;

-- verify INSERT OVERWRITE and INSERT INTO nonstandard partition location
ALTER TABLE parquet_events ADD PARTITION (run_date=201211, game_id=39, event_name='hq_change')
LOCATION '${hiveconf:test.blobstore.path.unique}/parquet_nonstd_partitions_loc/parquet_nonstd_loc/ns-part-1/';
INSERT OVERWRITE TABLE parquet_events PARTITION (run_date=201211, game_id=39, event_name='hq_change')
SELECT log_id,`time`,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201211';
SHOW PARTITIONS parquet_events;
SELECT COUNT(*) FROM parquet_events;
INSERT INTO TABLE parquet_events PARTITION (run_date=201211, game_id=39, event_name='hq_change')
SELECT log_id,`time`,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201211';
SHOW PARTITIONS parquet_events;
SELECT COUNT(*) FROM parquet_events;

SET hive.merge.mapfiles=false;

-- verify INSERT OVERWRITE and INSERT INTO nonstandard partition location with hive.merge.mapfiles false
ALTER TABLE parquet_events ADD PARTITION (run_date=201209, game_id=39, event_name='hq_change')
LOCATION '${hiveconf:test.blobstore.path.unique}/parquet_nonstd_partitions_loc/parquet_nonstd_loc/ns-part-2/';
INSERT OVERWRITE TABLE parquet_events PARTITION (run_date=201209, game_id=39, event_name='hq_change')
SELECT log_id,`time`,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209';
INSERT INTO TABLE parquet_events PARTITION (run_date=201209, game_id=39, event_name='hq_change')
SELECT log_id,`time`,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209';
SHOW PARTITIONS parquet_events;
SELECT COUNT(*) FROM parquet_events;

-- verify dynamic INSERT OVERWRITE over all partitions (standard and nonstandard locations) with hive.merge.mapfiles false
INSERT OVERWRITE TABLE parquet_events PARTITION (run_date, game_id, event_name)
SELECT * FROM src_events;
SHOW PARTITIONS parquet_events;
SELECT COUNT(*) FROM parquet_events;

SET hive.merge.mapfiles=true;

ALTER TABLE parquet_events ADD PARTITION (run_date=201207, game_id=39, event_name='hq_change')
LOCATION '${hiveconf:test.blobstore.path.unique}/parquet_nonstd_partitions_loc/parquet_nonstd_loc/ns-part-3/';
INSERT INTO TABLE parquet_events PARTITION (run_date=201207, game_id=39, event_name='hq_change')
SELECT log_id,`time`,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209';
SHOW PARTITIONS parquet_events;
SELECT COUNT(*) FROM parquet_events;

-- verify dynamic INSERT OVERWRITE over all partitions (standard and nonstandard locations) with hive.merge.mapfiles true
INSERT OVERWRITE TABLE parquet_events PARTITION (run_date, game_id, event_name)
SELECT * FROM src_events;
SHOW PARTITIONS parquet_events;
SELECT COUNT(*) FROM parquet_events;

ALTER TABLE parquet_events DROP PARTITION (run_date=201211, game_id=39, event_name='hq_change');
ALTER TABLE parquet_events DROP PARTITION (run_date=201209, game_id=39, event_name='hq_change');
ALTER TABLE parquet_events DROP PARTITION (run_date=201207, game_id=39, event_name='hq_change');
SHOW PARTITIONS parquet_events;
SELECT COUNT(*) FROM parquet_events;