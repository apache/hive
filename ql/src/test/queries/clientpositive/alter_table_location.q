drop table if exists hcat_altertable_16;
create table hcat_altertable_16(a int, b string) stored as textfile;
show table extended like  hcat_altertable_16;
alter table hcat_altertable_16 set location 'file:${system:test.tmp.dir}/hcat_altertable_16';
show table extended like  hcat_altertable_16;
