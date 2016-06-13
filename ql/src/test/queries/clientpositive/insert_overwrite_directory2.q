create external table result(key string) location "${system:test.tmp.dir}/result";

set mapreduce.job.reduces=2;

insert overwrite directory "${system:test.tmp.dir}/result"
select key from src group by key;

select count(*) from result;

set mapreduce.job.reduces=1;

insert overwrite directory "${system:test.tmp.dir}/result"
select key from src group by key;

select count(*) from result;

drop table result;