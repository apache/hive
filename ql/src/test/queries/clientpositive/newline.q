add file ../data/scripts/newline.py;
set hive.script.escape.newlines=true;

create table tmp_tmp(key string, value string) stored as rcfile;
insert overwrite table tmp_tmp
SELECT TRANSFORM(key, value) USING
'python newline.py' AS key, value FROM src limit 5;

select * from tmp_tmp;