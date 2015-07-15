SET hive.execution.engine=mr; 
SET hive.enforce.sortmergebucketmapjoin=false; 
SET hive.auto.convert.sortmerge.join=true; 
SET hive.optimize.bucketmapjoin = true; 
SET hive.optimize.bucketmapjoin.sortedmerge = true; 
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE data_table (key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'; 

insert into table data_table values(1, 'one');
insert into table data_table values(2, 'two');

CREATE TABLE smb_table (key INT, value STRING) CLUSTERED BY (key) 
SORTED BY (key) INTO 1 BUCKETS STORED AS ORC;

CREATE TABLE smb_table_part (key INT, value STRING) PARTITIONED BY (p1 DECIMAL) 
CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS 
STORED AS ORC;

INSERT OVERWRITE TABLE smb_table SELECT * FROM data_table; 

INSERT OVERWRITE TABLE smb_table_part PARTITION (p1) SELECT key, value, 100 as p1 FROM data_table;

SELECT s1.key, s2.p1 FROM smb_table s1 INNER JOIN smb_table_part s2 ON s1.key = s2.key ORDER BY s1.key;

drop table smb_table_part;

CREATE TABLE smb_table_part (key INT, value STRING) PARTITIONED BY (p1 double) 
CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS 
STORED AS ORC;

INSERT OVERWRITE TABLE smb_table_part PARTITION (p1) SELECT key, value, 100 as p1 FROM data_table;

SELECT s1.key, s2.p1 FROM smb_table s1 INNER JOIN smb_table_part s2 ON s1.key = s2.key ORDER BY s1.key;