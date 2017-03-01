create database escape_comments_db comment 'a\nb';
use escape_comments_db;
create table escape_comments_tbl1
(col1 string comment 'a\nb\'\;') comment 'a\nb'
partitioned by (p1 string comment 'a\nb');
create view escape_comments_view1 (col1 comment 'a\nb') comment 'a\nb'
as select col1 from escape_comments_tbl1;
create index index2 on table escape_comments_tbl1(col1) as 'COMPACT' with deferred rebuild comment 'a\nb';

describe database extended escape_comments_db;
describe database escape_comments_db;
show create table escape_comments_tbl1;
describe formatted escape_comments_tbl1;
describe pretty escape_comments_tbl1;
describe escape_comments_tbl1;
show create table escape_comments_view1;
describe formatted escape_comments_view1;
show formatted index on escape_comments_tbl1;

drop database escape_comments_db cascade;
