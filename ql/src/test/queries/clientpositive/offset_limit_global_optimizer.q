--! qt:dataset:srcpart
set hive.limit.optimize.enable=true;
set hive.limit.row.max.size=12;
set hive.mapred.mode=nonstrict;

EXPLAIN EXTENDED
SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 400,10;

SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 400,10;

EXPLAIN EXTENDED
SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,10;

SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,10;

EXPLAIN EXTENDED
SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,20;

SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,20;

EXPLAIN EXTENDED
SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,600;

SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,600;

set hive.cbo.enable=false;

EXPLAIN EXTENDED
SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 400,10;

SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 400,10;

EXPLAIN EXTENDED
SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,10;

SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,10;

EXPLAIN EXTENDED
SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,20;

SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,20;

EXPLAIN EXTENDED
SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,600;

SELECT srcpart.key, substr(srcpart.value,5) as csubstr, ds, hr FROM srcpart ORDER BY srcpart.key, csubstr, ds, hr LIMIT 490,600;