add jar ../build/contrib/hive_contrib.jar;

DROP TABLE s3log;
CREATE TABLE s3log
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.s3.S3LogDeserializer'
STORED AS TEXTFILE;

DESCRIBE s3log;

LOAD DATA LOCAL INPATH '../contrib/data/files/s3.log' INTO TABLE s3log;

SELECT a.* FROM s3log a;

DROP TABLE s3log;
