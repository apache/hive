--! qt:dataset:src
DROP VIEW xxx3;
CREATE VIEW xxx3 AS SELECT * FROM src;
ALTER TABLE xxx3 REPLACE COLUMNS (xyz int);
