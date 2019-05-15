-- Test the case HIVE-20331 with the union all, lateral view and join
DROP TABLE IF EXISTS unionall_lateralview1;
DROP TABLE IF EXISTS unionall_lateralview2;
CREATE TABLE unionall_lateralview1(col1 INT);
INSERT INTO unionall_lateralview1 VALUES(1), (2);
CREATE TABLE unionall_lateralview2(col1 INT);

INSERT INTO unionall_lateralview2
SELECT 1 AS `col1`
FROM unionall_lateralview1
UNION ALL
  SELECT 2 AS `col1`
  FROM
    (SELECT col1
     FROM unionall_lateralview1
    ) x1
    JOIN
      (SELECT col1
      FROM
        (SELECT
          Row_Number() over (PARTITION BY col1 ORDER BY col1) AS `col1`
        FROM unionall_lateralview1
        ) x2 lateral VIEW explode(map(10,1))`mapObj` AS `col2`, `col3`
      ) `expdObj`;

SELECT * FROM unionall_lateralview2 ORDER BY col1;

DROP TABLE unionall_lateralview1;
DROP TABLE unionall_lateralview2;
