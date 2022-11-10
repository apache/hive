--! qt:sysdb
set hive.metastore.event.listeners=org.apache.hive.hcatalog.listener.DbNotificationListener;
set hive.metastore.dml.events=true;

CREATE EXTERNAL TABLE exttable (b INT) PARTITIONED BY (a INT) STORED AS ORC;

INSERT INTO TABLE exttable PARTITION (a) VALUES (1,2), (2,3), (3,4), (4,5), (5,6);

SELECT COUNT(*) FROM sys.notification_log WHERE tbl_name='exttable' AND event_type='INSERT';

INSERT INTO TABLE exttable PARTITION (a) VALUES (1,2), (2,3), (3,4), (4,5), (5,6);

SELECT COUNT(*) FROM sys.notification_log WHERE tbl_name='exttable' AND event_type='INSERT';

DROP TABLE exttable;