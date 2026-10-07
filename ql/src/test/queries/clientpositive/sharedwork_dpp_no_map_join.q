set hive.auto.convert.join=false;

CREATE TABLE swo_hang (
  a_c string,
  b_c int
)
PARTITIONED BY (
  day int
)
STORED AS ORC;

INSERT INTO swo_hang VALUES ('userid1', 1, 2);

explain SELECT
  u.day
FROM (
  SELECT
    v.day,
    v.a_c
  FROM (
    SELECT day,
      a_c,
      MIN(b_c)
    FROM swo_hang
    WHERE day >=1
      AND a_c IS NOT NULL
    GROUP BY day, a_c
  ) v
  JOIN (
    SELECT day,
      a_c,
      MIN(b_c)
    FROM swo_hang
    WHERE day >=1
      AND a_c IS NOT NULL
    GROUP BY day, a_c
  ) a
  ON v.a_c = a.a_c
    AND v.day = a.day
  LEFT JOIN (
    SELECT a_c
    FROM swo_hang
    WHERE day >=1
    AND a_c IS NOT NULL
  ) p
  ON v.a_c = p.a_c
  WHERE p.a_c IS NULL
) u;
