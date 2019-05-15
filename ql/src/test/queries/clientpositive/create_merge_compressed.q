set hive.strict.checks.bucketing=false;

create table src_rc_merge_test_n1(key int, value string) stored as rcfile;

load data local inpath '../../data/files/smbbucket_1.rc' into table src_rc_merge_test_n1;

set hive.exec.compress.output = true;

create table tgt_rc_merge_test_n1(key int, value string) stored as rcfile;
insert into table tgt_rc_merge_test_n1 select * from src_rc_merge_test_n1;
insert into table tgt_rc_merge_test_n1 select * from src_rc_merge_test_n1;

show table extended like `tgt_rc_merge_test_n1`;

select count(1) from tgt_rc_merge_test_n1;
select sum(hash(key)), sum(hash(value)) from tgt_rc_merge_test_n1;

alter table tgt_rc_merge_test_n1 concatenate;

show table extended like `tgt_rc_merge_test_n1`;

select count(1) from tgt_rc_merge_test_n1;
select sum(hash(key)), sum(hash(value)) from tgt_rc_merge_test_n1;

drop table src_rc_merge_test_n1;
drop table tgt_rc_merge_test_n1;