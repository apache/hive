set hive.auto.convert.join=false;

create table srcTable (key string, value string);

explain formatted
SELECT x.key, z.value, y.value
FROM srcTable x JOIN srcTable y ON (x.key = y.key) 
JOIN srcTable z ON (x.value = z.value);

explain formatted
SELECT x.key, z.value, y.value
FROM srcTable x JOIN srcTable y ON (x.key = y.key) 
JOIN (select * from srcTable union select * from srcTable)z ON (x.value = z.value)
union
SELECT x.key, z.value, y.value
FROM srcTable x JOIN srcTable y ON (x.key = y.key) 
JOIN (select * from srcTable union select * from srcTable)z ON (x.value = z.value);

