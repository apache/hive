create database unicode_comments_db comment '数据库';
use unicode_comments_db;
create table unicode_comments_tbl1
(col1 string comment '第一列') comment '表格'
partitioned by (p1 string comment '分割');
create view unicode_comments_view1 (col1 comment '第一列') comment '视图'
as select col1 from unicode_comments_tbl1;
create index index2 on table unicode_comments_tbl1(col1) as 'COMPACT' with deferred rebuild comment '索引';

describe database extended unicode_comments_db;
show create table unicode_comments_tbl1;
describe formatted unicode_comments_tbl1;
show create table unicode_comments_view1;
describe formatted unicode_comments_view1;
show formatted index on unicode_comments_tbl1;

drop database unicode_comments_db cascade;
