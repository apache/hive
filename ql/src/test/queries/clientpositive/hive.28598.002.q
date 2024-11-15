--! qt:disabled:hive28598.orc is too large.

set hive.explain.user=false;
set hive.log.explain.output=false;

set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.dynamic.semijoin.reduction.threshold=-1000;

CREATE TABLE source(col string, col2 string, col3 string) stored as orc;
LOAD DATA LOCAL INPATH '../../data/files/hive28598.orc' overwrite into table source;

CREATE TABLE source2(col string, col2 string, col3 string) stored as orc;
LOAD DATA LOCAL INPATH '../../data/files/hive28598.orc' overwrite into table source;
 
alter table source update statistics set('numRows'='28', 'rawDataSize'='81449');
alter table source2 update statistics set('numRows'='123456', 'rawDataSize'='1234567');

explain
select count(1) from 
(select col,col2 from source WHERE col3='NORMAL') t
left join
(select col,col2 from source2)  t2
on t.col = t2.col and t.col2 = t2.col2; 

select count(1) from 
(select col,col2 from source WHERE col3='NORMAL') t
left join
(select col,col2 from source2)  t2
on t.col = t2.col and t.col2 = t2.col2; 

