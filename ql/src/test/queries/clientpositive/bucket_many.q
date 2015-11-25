;
set mapred.reduce.tasks = 16;

create table bucket_many(key int, value string) clustered by (key) into 256 buckets;

explain extended
insert overwrite table bucket_many
select * from src;

insert overwrite table bucket_many
select * from src;

explain
select * from bucket_many tablesample (bucket 1 out of 256) s;

select * from bucket_many tablesample (bucket 1 out of 256) s;
