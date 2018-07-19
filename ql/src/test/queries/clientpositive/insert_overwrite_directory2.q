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
