create table test_limit_offset (id int);
insert into test_limit_offset values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16);
create table test_limit_offset2 as select * from test_limit_offset limit 5 offset 2;
select * from test_limit_offset2;