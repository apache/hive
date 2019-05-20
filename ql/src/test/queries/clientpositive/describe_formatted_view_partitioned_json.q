--! qt:dataset:src
set hive.ddl.output.format=json;

DROP VIEW view_partitioned_n0;

CREATE VIEW view_partitioned_n0
PARTITIONED ON (value)
AS
SELECT key, value
FROM src
WHERE key=86;

ALTER VIEW view_partitioned_n0
ADD PARTITION (value='val_86');

DESCRIBE FORMATTED view_partitioned_n0 PARTITION (value='val_86');

DROP VIEW view_partitioned_n0;
