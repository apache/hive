--! qt:dataset:srcpart
--! qt:dataset:src
CREATE DATABASE tv;
CREATE VIEW tv.testView as SELECT * FROM srcpart;
DESCRIBE FORMATTED tv.testView;

ALTER VIEW tv.testView AS SELECT value FROM src WHERE key=86;
DESCRIBE FORMATTED tv.testView;

ALTER VIEW tv.testView AS
SELECT * FROM src
WHERE key > 80 AND key < 100
ORDER BY key, value
LIMIT 10;
DESCRIBE FORMATTED tv.testView;

DROP VIEW tv.testView;
DROP DATABASE tv;