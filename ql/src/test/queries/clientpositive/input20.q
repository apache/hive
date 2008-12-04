CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  MAP src.key % 2, src.key % 5
  USING 'cat'
  CLUSTER BY key
) tmap
INSERT OVERWRITE TABLE dest1
REDUCE tmap.key, tmap.value
USING 'uniq -c | sed "s@^ *@@" | sed "s@\t@_@" | sed "s@ @\t@"'
AS key, value;

FROM (
  FROM src
  MAP src.key % 2, src.key % 5
  USING 'cat'
  CLUSTER BY key
) tmap
INSERT OVERWRITE TABLE dest1
REDUCE tmap.key, tmap.value
USING 'uniq -c | sed "s@^ *@@" | sed "s@\t@_@" | sed "s@ @\t@"'
AS key, value;
