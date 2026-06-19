--HIVE 6209

drop table target_n0;
drop table temp;

create table target_n0 (key string, value string) stored as textfile location 'file:${system:test.tmp.dir}/target';
create table temp (key string, value string) stored as textfile location 'file:${system:test.tmp.dir}/temp';

set fs.pfile.impl.disable.cache=false;

load data local inpath '../../data/files/kv1.txt' into table temp;
load data inpath '${system:test.tmp.dir}/temp/kv1.txt' overwrite into table target_n0;
select count(*) from target_n0;

load data local inpath '../../data/files/kv2.txt' into table temp;
load data inpath '${system:test.tmp.dir}/temp/kv2.txt' overwrite into table target_n0;
select count(*) from target_n0;

drop table target_n0;
drop table temp;