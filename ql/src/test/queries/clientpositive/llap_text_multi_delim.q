-- Verify that after wiring MultiDelimitSerDe into VectorDeserializeOrcWriter,
-- the LazySimpleSerDe (single-byte) fast path still works unchanged across
-- several delimiter shapes, and the new MultiDelimitSerDe (multi-byte) path
-- produces identical results for equivalent data.
set hive.llap.io.enabled=true;
set hive.llap.io.encode.enabled=true;
set hive.llap.io.encode.vector.serde.enabled=true;
set hive.llap.io.encode.vector.serde.async.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS lazy_simple_llap;
DROP TABLE IF EXISTS lazy_simple_comma_llap;
DROP TABLE IF EXISTS lazy_simple_escape_llap;
DROP TABLE IF EXISTS multi_delim_llap;
DROP TABLE IF EXISTS multi_delim_escape_llap;

-- ---------------------------------------------------------------------------
-- LazySimpleSerDe: single-byte '|' — the classic hot loop through
-- VectorDeserializeOrcWriter → LazySimpleDeserializeRead. Nothing changed on
-- this path; the queries below assert it still parses cleanly and vectorizes.
-- ---------------------------------------------------------------------------
CREATE TABLE lazy_simple_llap(id INT, name STRING, val INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DESCRIBE FORMATTED lazy_simple_llap;

LOAD DATA LOCAL INPATH '../../data/files/single_delim.txt'
  INTO TABLE lazy_simple_llap;

SELECT * FROM lazy_simple_llap;
SELECT COUNT(*) FROM lazy_simple_llap;
SELECT SUM(val) FROM lazy_simple_llap;
SELECT COUNT(*) FROM lazy_simple_llap WHERE name IS NULL OR name = '';

-- ---------------------------------------------------------------------------
-- LazySimpleSerDe: a different single-byte delimiter (',') — proves the fast
-- path is not hardcoded to '|' and picks up whatever FIELD_DELIM says.
-- ---------------------------------------------------------------------------
CREATE TABLE lazy_simple_comma_llap(id INT, name STRING, val INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DESCRIBE FORMATTED lazy_simple_comma_llap;

LOAD DATA LOCAL INPATH '../../data/files/single_delim_comma.txt'
  INTO TABLE lazy_simple_comma_llap;

SELECT * FROM lazy_simple_comma_llap;
SELECT COUNT(*) FROM lazy_simple_comma_llap;
SELECT SUM(val) FROM lazy_simple_comma_llap;

-- ---------------------------------------------------------------------------
-- LazySimpleSerDe: '|' with escape.delim='\' — exercises the escape branch of
-- LazySimpleDeserializeRead (currentExternalBufferNeeded / copyToExternalBuffer)
-- through the LLAP encoder. Data contains '\|' inside fields which must be
-- unescaped to a literal '|', NOT treated as a field boundary.
-- ---------------------------------------------------------------------------
CREATE TABLE lazy_simple_escape_llap(id INT, name STRING, val INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'='|', 'escape.delim'='\\')
STORED AS TEXTFILE;

DESCRIBE FORMATTED lazy_simple_escape_llap;

LOAD DATA LOCAL INPATH '../../data/files/single_delim_escape.txt'
  INTO TABLE lazy_simple_escape_llap;

SELECT * FROM lazy_simple_escape_llap;
SELECT COUNT(*) FROM lazy_simple_escape_llap;
SELECT SUM(val) FROM lazy_simple_escape_llap;
-- The two fields with '|' inside them must survive as-is.
SELECT id, name FROM lazy_simple_escape_llap WHERE name LIKE '%|%';

-- ---------------------------------------------------------------------------
-- MultiDelimitSerDe: multi-byte '~|' — must now route through the same
-- VectorDeserializeOrcWriter path via the new fieldDelimMulti wiring.
-- ---------------------------------------------------------------------------
CREATE TABLE multi_delim_llap(id INT, name STRING, val INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ('field.delim'='~|')
STORED AS TEXTFILE;

DESCRIBE FORMATTED multi_delim_llap;

LOAD DATA LOCAL INPATH '../../data/files/multi_delim.txt'
  INTO TABLE multi_delim_llap;

SELECT * FROM multi_delim_llap;
SELECT COUNT(*) FROM multi_delim_llap;
SELECT SUM(val) FROM multi_delim_llap;

-- Parity check: LazySimple '|' and MultiDelimit '~|' carry the same rows.
SELECT COUNT(*) FROM (
  SELECT id, name, val FROM lazy_simple_llap
  EXCEPT
  SELECT id, name, val FROM multi_delim_llap
) diff1;

SELECT COUNT(*) FROM (
  SELECT id, name, val FROM multi_delim_llap
  EXCEPT
  SELECT id, name, val FROM lazy_simple_llap
) diff2;

-- ---------------------------------------------------------------------------
-- MultiDelimitSerDe + escape.delim must NOT take the fast path — the router
-- falls back to DeserializerOrcWriter — but the query must still succeed and
-- return the same rows.
-- ---------------------------------------------------------------------------
CREATE TABLE multi_delim_escape_llap(id INT, name STRING, val INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ('field.delim'='~|', 'escape.delim'='\\')
STORED AS TEXTFILE;

DESCRIBE FORMATTED multi_delim_escape_llap;

LOAD DATA LOCAL INPATH '../../data/files/multi_delim.txt'
  INTO TABLE multi_delim_escape_llap;

SELECT COUNT(*) FROM multi_delim_escape_llap;

DROP TABLE lazy_simple_llap;
DROP TABLE lazy_simple_comma_llap;
DROP TABLE lazy_simple_escape_llap;
DROP TABLE multi_delim_llap;
DROP TABLE multi_delim_escape_llap;
