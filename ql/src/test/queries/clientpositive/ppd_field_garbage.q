-- ppd leaves invalid expr in field expr
CREATE TABLE test_issue (fileid int, infos ARRAY<STRUCT<user:INT>>, test_c STRUCT<user_c:STRUCT<age:INT>>);
CREATE VIEW v_test_issue AS SELECT fileid, i.user, test_c.user_c.age FROM test_issue LATERAL VIEW explode(infos) info AS i;

-- dummy data
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE test_issue;

SELECT * FROM v_test_issue WHERE age = 25;
