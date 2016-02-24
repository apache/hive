set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION get_splits;
DESCRIBE FUNCTION EXTENDED get_splits;

select r.if_class as ic, r.split_class as sc, hash(r.split) as h, length(r.split) as l from (select explode(get_splits("select key, count(*) from src where key % 2 = 0 group by key",5)) as r) t;
