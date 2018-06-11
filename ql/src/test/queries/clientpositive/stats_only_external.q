--! qt:dataset:src

set hive.compute.query.using.stats=true;
set hive.optimize.filter.stats.reduction=true;

drop table if exists stats_only_external_tab1;
drop table if exists stats_only_external_tab1_ext;

create table stats_only_external_tab1 (key int, value string) stored as orc;
create external table stats_only_external_tab1_ext like stats_only_external_tab1 stored as orc;

insert into stats_only_external_tab1 select * from src;
insert into stats_only_external_tab1_ext select * from src;

analyze table stats_only_external_tab1 compute statistics for columns;
analyze table stats_only_external_tab1_ext compute statistics for columns;

set test.comment=Regular table should should compute using stats;
set test.comment;
explain select count(*) from stats_only_external_tab1;

set test.comment=External table should not should compute using stats;
set test.comment;
explain select count(*) from stats_only_external_tab1_ext;

set test.comment=Query predicates removed due to column stats;
set test.comment;
explain select count(*) from stats_only_external_tab1 where value is not null and key >= 0;

set test.comment=Predicate removal disabled for external tables;
set test.comment;
explain select count(*) from stats_only_external_tab1_ext where value is not null and key >= 0;

drop table stats_only_external_tab1;
drop table stats_only_external_tab1_ext;
