-- protect mode: syntax to change protect mode works and queries to drop partitions are blocked if it is marked no drop

create database if not exists db1;
use db1;

create table tbl_protectmode_no_drop2  (c1 string,c2 string) partitioned by (p string);
alter table tbl_protectmode_no_drop2 add partition (p='p1');
alter table tbl_protectmode_no_drop2 partition (p='p1') enable no_drop;

use default;
drop table db1.tbl_protectmode_no_drop2;
