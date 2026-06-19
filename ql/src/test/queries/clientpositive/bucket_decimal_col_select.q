set hive.tez.bucket.pruning=true;

create table bucket_table(id decimal(38,0), name string) clustered by(id) into 3 buckets;
insert into bucket_table values(5000000000000999640711, 'Cloud');

select * from bucket_table bt where id = 5000000000000999640711;