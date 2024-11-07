--! qt:dataset:src
create external table result_n0(key string) location "${system:test.tmp.dir}/result_n0";

set mapreduce.job.reduces=2;

insert overwrite directory "${system:test.tmp.dir}/result_n0"
select key from src group by key;

select count(*) from result_n0;

set mapreduce.job.reduces=1;

insert overwrite directory "${system:test.tmp.dir}/result_n0"
select key from src group by key;

select count(*) from result_n0;

drop table result_n0;

create external table iow_directory_src (c1 int) location "${system:test.tmp.dir}/iow_directory_src";
create external table iow_directory_dst (c1 int) location "${system:test.tmp.dir}/iow_directory_dst";
insert into iow_directory_dst (c1) values (1), (2), (3);
dfs -count ${system:test.tmp.dir}/iow_directory_dst;
dfs -count ${system:test.tmp.dir}/iow_directory_src;
insert overwrite directory "${system:test.tmp.dir}/iow_directory_dst" select * from iow_directory_src;
dfs -count ${system:test.tmp.dir}/iow_directory_dst;
drop table iow_directory_src;
drop table iow_directory_dst;
