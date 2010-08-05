-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode_no_drop;

create table tbl_protectmode_no_drop  (c1 string,c2 string) partitioned by (p string);
alter table tbl_protectmode_no_drop add partition (p='p1');
alter table tbl_protectmode_no_drop partition (p='p1') enable no_drop;
desc extended tbl_protectmode_no_drop partition (p='p1');

drop table tbl_protectmode_no_drop;