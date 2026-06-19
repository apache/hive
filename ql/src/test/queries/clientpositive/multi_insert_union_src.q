--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
drop table if exists src2_n4;
drop table if exists src_multi1_n3;
drop table if exists src_multi2_n4;
set hive.stats.dbclass=fs;
CREATE TABLE src2_n4 as SELECT * FROM src;

create table src_multi1_n3 like src;
create table src_multi2_n4 like src;

explain
from (select * from src1 where key < 10 union all select * from src2_n4 where key > 100) s
insert overwrite table src_multi1_n3 select key, value where key < 150 order by key
insert overwrite table src_multi2_n4 select key, value where key > 400 order by value;

from (select * from src1 where key < 10 union all select * from src2_n4 where key > 100) s
insert overwrite table src_multi1_n3 select key, value where key < 150 order by key
insert overwrite table src_multi2_n4 select key, value where key > 400 order by value;

select * from src_multi1_n3;
select * from src_multi2_n4;
