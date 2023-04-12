SET hive.cli.print.header=true;

CREATE TABLE T1 (c1 int, c2 array<int>);
CREATE TABLE T2 AS SELECT * FROM T1 LIMIT 0;
DESCRIBE FORMATTED t2;
