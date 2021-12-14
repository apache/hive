create table tbl (fld int) partitioned by (fld1 int, fld2 int) STORED AS ORC;
insert into tbl values (1, 2, 3);
insert into tbl values (11, 12, 13);
analyze table tbl partition (fld1 = 2) COMPUTE STATISTICS FOR COLUMNS;
analyze table tbl partition (fld2 = 13) COMPUTE STATISTICS FOR COLUMNS;
analyze table tbl partition (fld1 = 12, fld2 = 13) COMPUTE STATISTICS FOR COLUMNS;
analyze table tbl partition (fld1 = 2, fld2 = 3) COMPUTE STATISTICS FOR COLUMNS;
analyze table tbl COMPUTE STATISTICS FOR COLUMNS;
analyze table tbl partition (fld1 = 100) COMPUTE STATISTICS FOR COLUMNS;

