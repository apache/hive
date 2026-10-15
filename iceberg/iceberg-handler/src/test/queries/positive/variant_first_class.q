-- VARIANT as a first-class type (HIVE-30170)
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

drop table if exists vfc_main;

CREATE EXTERNAL TABLE vfc_main (id INT, v VARIANT) STORED BY ICEBERG TBLPROPERTIES ('format-version'='3');

-- the type is variant everywhere, not struct<metadata:binary,value:binary>
DESCRIBE vfc_main;
SHOW CREATE TABLE vfc_main;

INSERT INTO vfc_main VALUES
(1, parse_json('{"a":1}')),
(2, parse_json('{"a":1}')),
(3, parse_json('"text"')),
(4, CAST(NULL AS VARIANT));

SELECT typeof(v) FROM vfc_main WHERE id = 1;

-- CAST to string: a JSON null gives NULL, a string its characters, binary its base64, other scalars their text,
-- objects and arrays their JSON
SELECT id, CAST(v AS STRING) FROM vfc_main ORDER BY id;
SELECT CAST(parse_json('null') AS STRING), CAST(parse_json('true') AS STRING), CAST(parse_json('1.5') AS STRING),
  CAST(parse_json('[1,"a"]') AS STRING), CAST(CAST(unhex('DEADBEEF') AS VARIANT) AS STRING),
  CAST(CAST(DATE '2026-07-28' AS VARIANT) AS STRING) FROM vfc_main WHERE id = 1;
-- an implicit conversion to string follows the same rule
CREATE TEMPORARY TABLE vfc_str (id INT, s STRING);
INSERT INTO vfc_str SELECT id, v FROM vfc_main;
SELECT id, s FROM vfc_str ORDER BY id;

-- IN and collect_set follow '=' (byte-wise)
SELECT id FROM vfc_main WHERE v IN (parse_json('{"a":1}'), parse_json('7')) ORDER BY id;
SELECT size(collect_set(v)) FROM vfc_main;

-- a NULL variant constant vectorizes, projected (the UNION ALL form of a bare NULL) or passed to a UDF
set hive.vectorized.execution.enabled=true;
EXPLAIN VECTORIZATION ONLY SELECT id, v FROM vfc_main UNION ALL SELECT id + 10, CAST(NULL AS VARIANT) FROM vfc_main;
SELECT id, v FROM vfc_main UNION ALL SELECT id + 10, CAST(NULL AS VARIANT) FROM vfc_main ORDER BY id;
EXPLAIN VECTORIZATION ONLY SELECT id, to_json(CAST(NULL AS VARIANT)), array(v, CAST(NULL AS VARIANT)) FROM vfc_main;
SELECT id, to_json(CAST(NULL AS VARIANT)), array(v, CAST(NULL AS VARIANT)) FROM vfc_main ORDER BY id;

-- parse_json of a literal is evaluated per row, not folded into a constant
EXPLAIN SELECT parse_json('{"k":1}') FROM vfc_main;

-- a temporary table may declare a VARIANT column
CREATE TEMPORARY TABLE vfc_tmp_decl (id INT, v VARIANT);
INSERT INTO vfc_tmp_decl SELECT id, v FROM vfc_main;
DESCRIBE vfc_tmp_decl;
SELECT id, v FROM vfc_tmp_decl ORDER BY id;

-- so may a partitioned one: a static-partition insert validates the partition, which is exempt too
CREATE TEMPORARY TABLE vfc_tmp_part (id INT, v VARIANT) PARTITIONED BY (p INT) STORED AS ORC;
INSERT INTO vfc_tmp_part PARTITION (p=1) SELECT id, v FROM vfc_main;
SELECT p, id, v FROM vfc_tmp_part ORDER BY id;

-- CTAS into Iceberg v3 creates a VARIANT column
CREATE TABLE vfc_ctas STORED BY ICEBERG TBLPROPERTIES ('format-version'='3') AS SELECT id, v FROM vfc_main;
DESCRIBE vfc_ctas;
SELECT id, v FROM vfc_ctas ORDER BY id;

-- a view exposes VARIANT
CREATE VIEW vfc_view AS SELECT id, v FROM vfc_main;
DESCRIBE vfc_view;
SELECT id, v FROM vfc_view ORDER BY id;

-- a shuffle join on a variant key merges both sides in their sort order; the map join gives the same rows
CREATE TEMPORARY TABLE vfc_join (id INT, v VARIANT);
INSERT INTO vfc_join VALUES
(1, parse_json('{"a":1}')), (2, parse_json('1')), (3, parse_json('-5')), (4, parse_json('200')),
(5, parse_json('{"a":1}')), (6, parse_json('-5')), (7, parse_json('200')), (8, parse_json('1')),
(9, CAST(NULL AS VARIANT));
set hive.auto.convert.join=false;
EXPLAIN SELECT a.id, b.id FROM vfc_join a JOIN vfc_join b ON a.v = b.v WHERE a.id < b.id ORDER BY a.id, b.id;
SELECT a.id, b.id FROM vfc_join a JOIN vfc_join b ON a.v = b.v WHERE a.id < b.id ORDER BY a.id, b.id;
set hive.auto.convert.join=true;
SELECT a.id, b.id FROM vfc_join a JOIN vfc_join b ON a.v = b.v WHERE a.id < b.id ORDER BY a.id, b.id;

-- CTE materialization takes the temporary-table path
set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.materialize.full.aggregate.only=false;
EXPLAIN WITH c AS (SELECT id, v FROM vfc_main) SELECT c1.id, c2.v FROM c c1 JOIN c c2 ON c1.id = c2.id;
WITH c AS (SELECT id, v FROM vfc_main) SELECT c1.id, c2.v FROM c c1 JOIN c c2 ON c1.id = c2.id ORDER BY c1.id;

-- the shuffle (GROUP BY, DISTINCT, ORDER BY, windows), hash and conditionals over Iceberg values. Bytes >= 0x80
-- (-5 is int8 0xfb, 200 and -200 are int16 with a high byte, "é" is UTF-8 0xc3 0xa9) show a signed byte compare or
-- hash; JSON null is a value, SQL NULL is not
CREATE EXTERNAL TABLE vfc_hi (id INT, v VARIANT) STORED BY ICEBERG TBLPROPERTIES ('format-version'='3');
INSERT INTO vfc_hi VALUES
(1, parse_json('1')), (2, parse_json('-5')), (3, parse_json('200')), (4, parse_json('-200')), (5, parse_json('"é"')),
(6, parse_json('"e"')), (7, parse_json('{"a":1,"b":"x"}')), (8, parse_json('[1,2,3]')), (9, parse_json('-5')),
(10, parse_json('null')), (11, CAST(NULL AS VARIANT));
set hive.vectorized.execution.enabled=false;
SELECT v, count(*) FROM vfc_hi GROUP BY v ORDER BY v;
SELECT id, v, hash(v) FROM vfc_hi ORDER BY v DESC, id;
SELECT count(v), count(DISTINCT v), min(v), max(v) FROM vfc_hi;
SELECT id, rank() OVER (PARTITION BY v ORDER BY id), first_value(v) OVER (PARTITION BY id % 3 ORDER BY id)
FROM vfc_hi ORDER BY id;
SELECT id FROM vfc_hi WHERE v IS NULL OR v = parse_json('-5') ORDER BY id;
SELECT id, COALESCE(v, parse_json('0')), IF(id > 9, v, NULL), CASE WHEN id = 1 THEN v END, NVL(v, parse_json('0'))
FROM vfc_hi ORDER BY id;
-- vectorized: the sort with hash, and the windows. GROUP BY on a variant key does not vectorize, and vectorized IF/CASE
-- over a variant fails when the condition is repeating or all true (HIVE-30188)
set hive.vectorized.execution.enabled=true;
SELECT id, v, hash(v) FROM vfc_hi ORDER BY v DESC, id;
SELECT id, rank() OVER (PARTITION BY v ORDER BY id), first_value(v) OVER (PARTITION BY id % 3 ORDER BY id)
FROM vfc_hi ORDER BY id;

-- map join: the variant is a value of the small table. Joined on id: a join on an Iceberg variant key fails under
-- dynamic partition pruning (HIVE-30190)
SELECT a.id, a.v, b.v FROM vfc_main a JOIN vfc_hi b ON a.id = b.id ORDER BY a.id;

-- a temporary table created by CTAS, stored as ORC
CREATE TEMPORARY TABLE vfc_tmp_orc STORED AS ORC AS SELECT id, v FROM vfc_hi;
SELECT id, v FROM vfc_tmp_orc ORDER BY id;

-- a TEXTFILE directory holds the two binaries of each value, read back as the struct
INSERT OVERWRITE DIRECTORY '${system:test.tmp.dir}/vfc_hi_textfile' STORED AS TEXTFILE SELECT id, v FROM vfc_hi;
CREATE EXTERNAL TABLE vfc_hi_back (id INT, v STRUCT<metadata:BINARY, value:BINARY>) STORED AS TEXTFILE
LOCATION '${system:test.tmp.dir}/vfc_hi_textfile';
SELECT id, base64(v.metadata), base64(v.value) FROM vfc_hi_back ORDER BY id;
