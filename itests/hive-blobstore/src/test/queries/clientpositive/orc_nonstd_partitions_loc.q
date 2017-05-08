-- Test table in orc format with non-standard partition locations in blobstore

DROP TABLE src_events;
CREATE TABLE src_events
(
  log_id      BIGINT,
  time        BIGINT,
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
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_nonstd_partitions_loc/src_events/';
LOAD DATA LOCAL INPATH '../../data/files/events.txt' INTO TABLE src_events;

DROP TABLE orc_events;
CREATE TABLE orc_events
(
  log_id      BIGINT,
  time        BIGINT,
  uid         BIGINT,
  user_id     BIGINT,
  type        INT,
  event_data  STRING,
  session_id  STRING,
  full_uid    BIGINT
)
PARTITIONED BY (run_date STRING, game_id INT, event_name STRING)
STORED AS ORC
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_nonstd_partitions_loc/orc_events/';

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE orc_events PARTITION (run_date, game_id, event_name)
SELECT * FROM src_events;
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;

-- verify INSERT OVERWRITE and INSERT INTO nonstandard partition location
ALTER TABLE orc_events ADD PARTITION (run_date=201211, game_id=39, event_name='hq_change')
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_nonstd_partitions_loc/orc_nonstd_loc/ns-part-1/';
INSERT OVERWRITE TABLE orc_events PARTITION (run_date=201211, game_id=39, event_name='hq_change')
SELECT log_id,time,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201211';
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;
INSERT INTO TABLE orc_events PARTITION (run_date=201211, game_id=39, event_name='hq_change')
SELECT log_id,time,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201211';
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;

SET hive.merge.mapfiles=false;

-- verify INSERT OVERWRITE and INSERT INTO nonstandard partition location with hive.merge.mapfiles false
ALTER TABLE orc_events ADD PARTITION (run_date=201209, game_id=39, event_name='hq_change')
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_nonstd_partitions_loc/orc_nonstd_loc/ns-part-2/';
INSERT OVERWRITE TABLE orc_events PARTITION (run_date=201209, game_id=39, event_name='hq_change')
SELECT log_id,time,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209';
INSERT INTO TABLE orc_events PARTITION (run_date=201209, game_id=39, event_name='hq_change')
SELECT log_id,time,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209';
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;

-- verify dynamic INSERT OVERWRITE over all partitions (standard and nonstandard locations) with hive.merge.mapfiles false
INSERT OVERWRITE TABLE orc_events PARTITION (run_date, game_id, event_name)
SELECT * FROM src_events;
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;

SET hive.merge.mapfiles=true;

ALTER TABLE orc_events ADD PARTITION (run_date=201207, game_id=39, event_name='hq_change')
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_nonstd_partitions_loc/orc_nonstd_loc/ns-part-3/';
INSERT INTO TABLE orc_events PARTITION (run_date=201207, game_id=39, event_name='hq_change')
SELECT log_id,time,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209';
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;

-- verify dynamic INSERT OVERWRITE over all partitions (standard and nonstandard locations) with hive.merge.mapfiles true
INSERT OVERWRITE TABLE orc_events PARTITION (run_date, game_id, event_name)
SELECT * FROM src_events;
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;

ALTER TABLE orc_events DROP PARTITION (run_date=201211, game_id=39, event_name='hq_change');
ALTER TABLE orc_events DROP PARTITION (run_date=201209, game_id=39, event_name='hq_change');
ALTER TABLE orc_events DROP PARTITION (run_date=201207, game_id=39, event_name='hq_change');
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;