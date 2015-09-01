DROP TABLE IF EXISTS user_web_events;
create temporary table user_web_events(`user id` bigint, `user name` string);

explain analyze table user_web_events compute statistics for columns;
analyze table user_web_events compute statistics for columns;

explain analyze table user_web_events compute statistics for columns `user id`;
analyze table user_web_events compute statistics for columns `user id`;
