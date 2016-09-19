set hive.cbo.enable=false;
DROP VIEW IF EXISTS a_view;

DROP TABLE IF EXISTS table_a1;
DROP TABLE IF EXISTS table_a2;
DROP TABLE IF EXISTS table_b1;
DROP TABLE IF EXISTS table_b2;

CREATE TABLE table_a1
(composite_key STRING);

CREATE TABLE table_a2
(composite_key STRING);

CREATE TABLE table_b1
(composite_key STRING, col1 STRING);

CREATE TABLE table_b2
(composite_key STRING);

CREATE VIEW a_view AS
SELECT
substring(a1.composite_key, 1, locate('|',a1.composite_key) - 1) AS autoname,
cast(NULL as string) AS col1
FROM table_a1 a1
FULL OUTER JOIN table_a2 a2
ON a1.composite_key = a2.composite_key
UNION ALL
SELECT
substring(b1.composite_key, 1, locate('|',b1.composite_key) - 1) AS autoname,
b1.col1 AS col1
FROM table_b1 b1
FULL OUTER JOIN table_b2 b2
ON b1.composite_key = b2.composite_key;

INSERT INTO TABLE table_b1
SELECT * FROM (
SELECT 'something|awful', 'col1'
)s ;

SELECT autoname FROM a_view WHERE autoname='something';
