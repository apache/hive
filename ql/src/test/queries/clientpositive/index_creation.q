drop index src_index_2 on src;
drop index src_index_3 on src;
drop index src_index_4 on src;
drop index src_index_5 on src;
drop index src_index_6 on src;
drop index src_index_7 on src;

create index src_index_2 on table src(key) as 'compact' WITH DEFERRED REBUILD;
desc extended default__src_src_index_2__;

create index src_index_3 on table src(key) as 'compact' WITH DEFERRED REBUILD in table src_idx_src_index_3;
desc extended src_idx_src_index_3;

create index src_index_4 on table src(key) as 'compact' WITH DEFERRED REBUILD ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
desc extended default__src_src_index_4__;

create index src_index_5 on table src(key) as 'compact' WITH DEFERRED REBUILD ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ESCAPED BY '\\';
desc extended default__src_src_index_5__;

create index src_index_6 on table src(key) as 'compact' WITH DEFERRED REBUILD STORED AS RCFILE;
desc extended default__src_src_index_6__;

create index src_index_7 on table src(key) as 'compact' WITH DEFERRED REBUILD in table src_idx_src_index_7 STORED AS RCFILE; 
desc extended src_idx_src_index_7;

drop index src_index_2 on src;
drop index src_index_3 on src;
drop index src_index_4 on src;
drop index src_index_5 on src;
drop index src_index_6 on src;
drop index src_index_7 on src;

show tables;
