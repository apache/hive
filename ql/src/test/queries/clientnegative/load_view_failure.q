--! qt:dataset:src
DROP VIEW xxx11;
CREATE VIEW xxx11 AS SELECT * FROM src;
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE xxx11;
