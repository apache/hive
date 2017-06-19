-- Test INSERT INTO and INSERT OVERWRITE on partitioned rcfile table in blobstore

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
LOCATION '${hiveconf:test.blobstore.path.unique}/rcfile_format_part/src_events/';
LOAD DATA LOCAL INPATH '../../data/files/events.txt' INTO TABLE src_events;

DROP TABLE rcfile_events;
CREATE TABLE rcfile_events
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
STORED AS RCFILE
LOCATION '${hiveconf:test.blobstore.path.unique}/rcfile_format_part/rcfile_events';

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE rcfile_events PARTITION (run_date, game_id, event_name)
SELECT * FROM src_events;
SHOW PARTITIONS rcfile_events;
SELECT COUNT(*) FROM rcfile_events;
SELECT COUNT(*) FROM rcfile_events WHERE run_date=20120921;
SELECT COUNT(*) FROM rcfile_events WHERE run_date=20121121;

INSERT OVERWRITE TABLE rcfile_events PARTITION (run_date=201211, game_id, event_name)
SELECT log_id,time,uid,user_id,type,event_data,session_id,full_uid,game_id,event_name FROM src_events
WHERE SUBSTR(run_date,1,6)='201211';
SHOW PARTITIONS rcfile_events;
SELECT COUNT(*) FROM rcfile_events;

INSERT INTO TABLE rcfile_events PARTITION (run_date=201209, game_id=39, event_name)
SELECT log_id,time,uid,user_id,type,event_data,session_id,full_uid,event_name FROM src_events
WHERE SUBSTR(run_date,1,6)='201209' AND game_id=39;
SELECT COUNT(*) FROM rcfile_events;

INSERT INTO TABLE rcfile_events PARTITION (run_date=201209, game_id=39, event_name='hq_change')
SELECT log_id,time,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209' AND game_id=39 AND event_name='hq_change';
SELECT COUNT(*) FROM rcfile_events;

INSERT OVERWRITE TABLE rcfile_events PARTITION (run_date=201209, game_id=39, event_name='hq_change')
SELECT log_id,time,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209' AND game_id=39 AND event_name='hq_change';
SELECT COUNT(*) FROM rcfile_events;