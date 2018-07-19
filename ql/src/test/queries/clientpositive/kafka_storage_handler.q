SET hive.vectorized.execution.enabled=false;

CREATE EXTERNAL TABLE kafka_table
(`__time` timestamp , `page` string, `user` string, `language` string,
`country` string,`continent` string, `namespace` string, `newPage` boolean, `unpatrolled` boolean,
`anonymous` boolean, `robot` boolean, added int, deleted int, delta bigint)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
WITH SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd\'T\'HH:mm:ss\'Z\'")
TBLPROPERTIES
("kafka.topic" = "test-topic",
"kafka.bootstrap.servers"="localhost:9092",
"kafka.serde.class"="org.apache.hadoop.hive.serde2.JsonSerDe")
;

DESCRIBE EXTENDED kafka_table;

Select * FROM kafka_table;

Select count(*) FROM kafka_table;

Select `__partition`, `__offset`,`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
from kafka_table where `__timestamp` > 1533960760123;
Select `__partition`, `__offset`, `__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
from kafka_table where `__timestamp` > 533960760123;

Select `__partition`, `__offset`,`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
from kafka_table where `__offset` > 7 and `__partition` = 0 OR
`__offset` = 4 and `__partition` = 0 OR `__offset` <= 1 and `__partition` = 0;

CREATE EXTERNAL TABLE kafka_table_2
(`__time` timestamp with local time zone , `page` string, `user` string, `language` string,
`country` string,`continent` string, `namespace` string, `newPage` boolean, `unpatrolled` boolean,
`anonymous` boolean, `robot` boolean, added int, deleted int, delta bigint)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES
("kafka.topic" = "test-topic",
"kafka.bootstrap.servers"="localhost:9092");

Select `__partition`, `__offset`, `__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
FROM kafka_table_2;

Select count(*) FROM kafka_table_2;