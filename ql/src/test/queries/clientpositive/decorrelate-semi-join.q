DROP TABLE IF EXISTS `tab`;
CREATE EXTERNAL TABLE `tab`(
  `f1` string,
  `f2` string,
  `f3` string,
  `f4` string,
  `f5` string,
  `f6` string);

SELECT 1
FROM tab a
WHERE a.f4 IN ('1', '2')
AND EXISTS (
  SELECT 1
  FROM tab b
  WHERE  a.f6 = b.f1 AND b.f3 IN (SELECT 1)
);
