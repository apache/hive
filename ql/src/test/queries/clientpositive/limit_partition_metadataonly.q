set hive.limit.query.max.table.partition=1;

-- SORT_QUERY_RESULTS

explain select ds from srcpart where hr=11 and ds='2008-04-08';
select ds from srcpart where hr=11 and ds='2008-04-08';

explain select distinct hr from srcpart;
select distinct hr from srcpart;
