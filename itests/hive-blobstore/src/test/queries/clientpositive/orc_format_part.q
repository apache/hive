-- Test INSERT INTO and INSERT OVERWRITE on partitioned orc table in blobstore

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
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_format_part/src_events/';
LOAD DATA LOCAL INPATH '../../data/files/events.txt' INTO TABLE src_events;

DROP TABLE orc_events;
CREATE TABLE orc_events
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
STORED AS ORC
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_format_part/orc_events';

SET hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE orc_events PARTITION (run_date, game_id, event_name)
SELECT * FROM src_events;
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;
SELECT COUNT(*) FROM orc_events WHERE run_date=20120921;
SELECT COUNT(*) FROM orc_events WHERE run_date=20121121;

INSERT OVERWRITE TABLE orc_events PARTITION (run_date=201211, game_id, event_name)
SELECT log_id,`time`,uid,user_id,type,event_data,session_id,full_uid,game_id,event_name FROM src_events
WHERE SUBSTR(run_date,1,6)='201211';
SHOW PARTITIONS orc_events;
SELECT COUNT(*) FROM orc_events;

INSERT INTO TABLE orc_events PARTITION (run_date=201209, game_id=39, event_name)
SELECT log_id,`time`,uid,user_id,type,event_data,session_id,full_uid,event_name FROM src_events
WHERE SUBSTR(run_date,1,6)='201209' AND game_id=39;
SELECT COUNT(*) FROM orc_events;

INSERT INTO TABLE orc_events PARTITION (run_date=201209, game_id=39, event_name='hq_change')
SELECT log_id,`time`,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209' AND game_id=39 AND event_name='hq_change';
SELECT COUNT(*) FROM orc_events;

INSERT OVERWRITE TABLE orc_events PARTITION (run_date=201209, game_id=39, event_name='hq_change')
SELECT log_id,`time`,uid,user_id,type,event_data,session_id,full_uid FROM src_events
WHERE SUBSTR(run_date,1,6)='201209' AND game_id=39 AND event_name='hq_change';
SELECT COUNT(*) FROM orc_events;