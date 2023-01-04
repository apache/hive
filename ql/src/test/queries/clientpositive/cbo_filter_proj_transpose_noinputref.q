create table test1 (s string);
create table test2 (m string);

EXPLAIN CBO SELECT c.m
FROM (
  SELECT substr(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), 1, 1) as m
  FROM test1
  WHERE substr(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), 1, 1) = '2') c
JOIN test2 d ON c.m = d.m;
