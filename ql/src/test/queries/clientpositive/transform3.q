CREATE TABLE transform3_t1 (col string);
INSERT OVERWRITE TABLE transform3_t1 VALUES('aaaa');

SELECT t.newCol FROM (
  SELECT TRANSFORM(col) USING 'cat' AS (NewCol string) FROM transform3_t1
) t;
