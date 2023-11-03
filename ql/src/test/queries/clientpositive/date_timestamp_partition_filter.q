--! qt:timezone:Asia/Hong_Kong

CREATE EXTERNAL TABLE testpd(col1 string, col2 String) PARTITIONED BY(PartitionDate DATE) STORED AS ORC;
INSERT into testpd(PartitionDate, col1, col2) VALUES('2023-01-01','Value11','Value12');
INSERT into testpd(PartitionDate, col1, col2) VALUES('2023-01-02','Value21','Value22');
explain extended select * from testpd where PartitionDate = '2023-01-01';
select * from testpd where PartitionDate = '2023-01-01';


CREATE EXTERNAL TABLE testpt(col1 string, col2 String) PARTITIONED BY(PartitionTimestamp TIMESTAMP) STORED AS ORC;
INSERT into testpt(PartitionTimestamp, col1, col2) VALUES('2023-01-01 10:20:30','Value11','Value12');
INSERT into testpt(PartitionTimestamp, col1, col2) VALUES('2023-01-02 20:30:40','Value21','Value22');
explain extended select * from testpt where PartitionTimestamp = '2023-01-01 10:20:30';
select * from testpt where PartitionTimestamp = '2023-01-01 10:20:30';
