-- Row positions are absolute within a data file. Variant row-group pruning must not shift them: a row
-- group the predicate drops still occupies its rows, so every later row group keeps the position it had.
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

drop table if exists variant_row_position;

CREATE EXTERNAL TABLE variant_row_position (
  id INT,
  data VARIANT
) STORED BY ICEBERG
TBLPROPERTIES (
  'format-version'='3',
  'variant.shredding.enabled'='true',
  'write.parquet.row-group-size-bytes'='1024'
);

-- The first rows carry tier=bronze, the later ones tier=gold, and the small row group size puts them in
-- different row groups. A predicate on tier drops the bronze ones, which is what shifts the positions of
-- the gold ones when pruning is applied to the footer the reader counts over.
INSERT INTO variant_row_position
SELECT pos, parse_json(concat('{"tier": "', if(pos < 200, 'bronze', 'gold'), '", "n": ', pos, '}'))
FROM (SELECT 1) x LATERAL VIEW posexplode(split(space(399), ' ')) e AS pos, val;

-- ROW__POSITION of the surviving rows must match their id, which was written in file order.
SELECT id, variant_row_position.ROW__POSITION
FROM variant_row_position
WHERE variant_get(data, '$.tier', 'string') = 'gold' AND id < 205
ORDER BY id;

-- the lowest surviving position is the first gold row, not zero
SELECT min(variant_row_position.ROW__POSITION) AS first_gold_position,
       max(variant_row_position.ROW__POSITION) AS last_gold_position,
       count(*) AS gold_rows
FROM variant_row_position
WHERE variant_get(data, '$.tier', 'string') = 'gold';

drop table variant_row_position;
