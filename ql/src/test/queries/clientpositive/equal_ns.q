set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table test(x string, y string);
insert into test values ('q', 'q'), ('q', 'w'), (NULL, 'q'), ('q', NULL), (NULL, NULL);
select *, x<=>y, not (x<=> y), (x <=> y) = false from test;
