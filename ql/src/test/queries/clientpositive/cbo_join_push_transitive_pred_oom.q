create table test1 (a string);
create table test2 (m string);
create table test3 (m string);

EXPLAIN CBO SELECT c.m
FROM
 (SELECT substr(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), 1, 1) AS m
  FROM test1
  UNION ALL
  SELECT m
  FROM test2
  WHERE m = '2') c
JOIN test3 d ON c.m = d.m;
