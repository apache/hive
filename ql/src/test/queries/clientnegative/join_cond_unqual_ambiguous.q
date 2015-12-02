set hive.mapred.mode=nonstrict;


explain select s1.key, s2.key
from src s1, src s2
where key = s2.key
;