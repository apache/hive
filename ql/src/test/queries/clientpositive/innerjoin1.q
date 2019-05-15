set hive.mapred.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

DROP TABLE IF EXISTS innerjoinsrc1;
DROP TABLE IF EXISTS innerjoinsrc2;

CREATE TABLE innerjoinsrc1 (a1 STRING, a2 STRING);
CREATE TABLE innerjoinsrc2 (b1 STRING, b2 STRING);

INSERT INTO TABLE innerjoinsrc1 (a1) VALUES ('1');
INSERT INTO TABLE innerjoinsrc1 VALUES ('2', '2');
INSERT INTO TABLE innerjoinsrc2 (b1) VALUES ('1');
INSERT INTO TABLE innerjoinsrc2 VALUES ('2', '2');

EXPLAIN SELECT * FROM innerjoinsrc1 c1, innerjoinsrc2 c2 WHERE COALESCE(a1,a2)=COALESCE(b1,b2);
SELECT * FROM innerjoinsrc1 c1, innerjoinsrc2 c2 WHERE COALESCE(a1,a2)=COALESCE(b1,b2);

EXPLAIN SELECT * FROM innerjoinsrc1 c1 inner join innerjoinsrc2 c2 ON (COALESCE(a1,a2)=COALESCE(b1,b2));
SELECT * FROM innerjoinsrc1 c1 inner join innerjoinsrc2 c2 ON (COALESCE(a1,a2)=COALESCE(b1,b2));