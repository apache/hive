set hive.fetch.task.conversion=none;
set hive.cbo.fallback.strategy=NEVER;

-- setting up a table with multiple rows
drop table if exists annotate_stats_lateral_view_join_test;
create table annotate_stats_lateral_view_join_test (id int, name string);
insert into annotate_stats_lateral_view_join_test (id, name) values
    (1, 'aaaaaaaaaa'),
    (2, 'bbbbbbbbbbbbbbbbbbbb'),
    (3, 'cccccccccccccccccccccccccccccc');

analyze table annotate_stats_lateral_view_join_test compute statistics;
analyze table annotate_stats_lateral_view_join_test compute statistics for columns;

-- with column stats
set hive.stats.fetch.column.stats=true;

-- 10x tests
set hive.stats.udtf.factor=10;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select a.id, name as n, b.* from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c lateral view explode(array(1, 2)) d;

-- 0.5x tests
set hive.stats.udtf.factor=0.5;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select a.id, name as n, b.* from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c lateral view explode(array(1, 2)) d;

-- Default behaviour tests
set hive.stats.udtf.factor=1;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select a.id, name as n, b.* from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c lateral view explode(array(1, 2)) d;

-- without column stats
set hive.stats.fetch.column.stats=false;

-- 10x tests
set hive.stats.udtf.factor=10;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select a.id, name as n, b.* from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c lateral view explode(array(1, 2)) d;

-- 0.5x tests
set hive.stats.udtf.factor=0.5;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select a.id, name as n, b.* from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c lateral view explode(array(1, 2)) d;

-- Default behaviour tests
set hive.stats.udtf.factor=1;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select a.id, name as n, b.* from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c;
explain select * from annotate_stats_lateral_view_join_test a lateral view explode(array(1, 2, 3)) b lateral view explode(array(1)) c lateral view explode(array(1, 2)) d;
