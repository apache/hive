set hive.mapred.mode=nonstrict;
-- see HIVE-2382
create table invites_n0 (id int, foo int, bar int);
explain select * from (select foo, bar from (select bar, foo from invites_n0 c union all select bar, foo from invites_n0 d) b) a group by bar, foo having bar=1;
drop table invites_n0;