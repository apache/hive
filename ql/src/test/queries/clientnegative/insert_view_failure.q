--! qt:dataset:src
DROP VIEW xxx2;
CREATE VIEW xxx2 AS SELECT * FROM src;
INSERT OVERWRITE TABLE xxx2
SELECT key, value
FROM src;
