set hive.cbo.enable=true;
-- SORT_QUERY_RESULTS
explain
select count(*) from srcpart a join srcpart b on a.key = b.key and a.hr = b.hr join srcpart c on a.hr = c.hr and a.key = c.key;
select count(*) from srcpart a join srcpart b on a.key = b.key and a.hr = b.hr join srcpart c on a.hr = c.hr and a.key = c.key;
