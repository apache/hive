set hive.cbo.enable=true;

SELECT
  id,
  flg
FROM
  (SELECT 1 as id, 1 as flg) z
WHERE
  id >= 0
  AND flg = TRUE
;

SELECT
  id,
  flg
FROM
  (SELECT 1 as id, 1 as flg) z
WHERE
  id >= 0
  OR flg = TRUE
;
