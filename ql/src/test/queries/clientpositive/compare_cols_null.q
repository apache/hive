create table ccn_table(key int, value string);
insert into ccn_table values('123a', 't1'), ('123', 't2');

set hive.cbo.enable=false;
explain select * from ccn_table where key > '123a';
select * from ccn_table where key > '123a';

explain select * from ccn_table where key <=> '123a';
select * from ccn_table where key <=> '123a';

set hive.cbo.enable=true;
explain cbo select * from ccn_table where key > '123a';
select * from ccn_table where key > '123a';

explain cbo select * from ccn_table where key <=> '123a';
select * from ccn_table where key <=> '123a';

drop table ccn_table;