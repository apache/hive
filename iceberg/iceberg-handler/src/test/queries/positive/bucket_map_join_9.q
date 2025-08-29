set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.optimize.shared.work=false;

CREATE TABLE tbl (foid string, part string, id string) PARTITIONED BY SPEC(bucket(10, id), bucket(10, part)) STORED BY ICEBERG;
INSERT INTO tbl VALUES ('1234', 'PART_123', '1'), ('1235', 'PART_124', '2');

EXPLAIN
SELECT * FROM tbl JOIN tbl tbl2 ON tbl.id = tbl2.id AND tbl.part = tbl2.part;
SELECT * FROM tbl JOIN tbl tbl2 ON tbl.id = tbl2.id AND tbl.part = tbl2.part;
