CREATE TABLE myorctable(ts timestamp)
STORED AS ORC;

INSERT INTO myorctable VALUES ('1970-01-01 00:00:00');

SELECT * FROM myorctable;
