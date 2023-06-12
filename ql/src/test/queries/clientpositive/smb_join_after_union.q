-- SORT_QUERY_RESULTS

create external table hive1_tbl_data (COLUMID string,COLUMN_FN string,COLUMN_LN string,EMAIL string,COL_UPDATED_DATE timestamp, PK_COLUM string);
create external table hive2_tbl_data (COLUMID string,COLUMN_FN string,COLUMN_LN string,EMAIL string,COL_UPDATED_DATE timestamp, PK_COLUM string);
create external table hive3_tbl_data (COLUMID string,COLUMN_FN string,COLUMN_LN string,EMAIL string,COL_UPDATED_DATE timestamp, PK_COLUM string);
create external table hive4_tbl_data (COLUMID string,COLUMN_FN string,COLUMN_LN string,EMAIL string,COL_UPDATED_DATE timestamp, PK_COLUM string);

insert into table hive1_tbl_data select '00001','john','doe','john@hotmail.com','2014-01-01 12:01:02','4000-10000';
insert into table hive1_tbl_data select '00002','john','doe','john@hotmail.com','2014-01-01 12:01:02','4000-10000';
insert into table hive2_tbl_data select '00001','john','doe','john@hotmail.com','2014-01-01 12:01:02','00001';
insert into table hive2_tbl_data select '00002','john','doe','john@hotmail.com','2014-01-01 12:01:02','00001';

-- Reference, without SMB join
set hive.auto.convert.sortmerge.join=false;

select t.COLUMID from 
(select distinct t.COLUMID as COLUMID from (SELECT COLUMID FROM hive3_tbl_data UNION ALL SELECT COLUMID FROM hive1_tbl_data) t) t
left join
(select distinct t.COLUMID from (SELECT COLUMID FROM hive4_tbl_data UNION ALL SELECT COLUMID FROM hive2_tbl_data) t) t1
on t.COLUMID = t1.COLUMID where t1.COLUMID is null;

-- HIVE-27303
-- The following list is the expected OperatorGraph for the query.
-- (Path1)                           TS0-SEL1-UNION4
-- (Path2)                           TS2-SEL3-UNION4-GBY7-RS8-GBY9-MERGEJOIN40
-- (Path3)  TS11-FIL32-SEL13-UNION17
-- (Path4)  TS14-FIL33-SEL16-UNION17-GBY20-RS21-GBY22-DUMMYSTORE41-MERGEJOIN40-FIL27-SEL28-FS29
-- With previous implementation, HIVE-27303 issue happens in the following steps.
-- TezCompiler.generateTaskTree() traverses the OperatorGraph in the following order: Path1 -> Path2 -> Path3 -> Path4.
-- During traversing Path4, TezCompiler changes the Output of RS21 to merged ReduceWork(Root: GBY22, Name: Reduce7)
-- contained in MergeJoinWork(Root: GBY9, Name: Reduce3) for MERGEJOIN40.
-- The output should be adjusted to MergeJoinWork as a merged work is not a regular tez vertex.
-- But TezCompiler already visited RS21-GBY22 path during Path3 and cut the parent-child relationship between them.
-- Therefore, TezCompiler does not visit MERGEJOIN40 during Path4, and the output name of RS21 is configured to non-existent vertex.

-- Use SMB join
set hive.auto.convert.join=false;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;
set hive.disable.unsafe.external.table.operations=false;

-- Prevent using AntiJoin
set hive.auto.convert.anti.join=false;

explain
select t.COLUMID from 
(select distinct t.COLUMID as COLUMID from (SELECT COLUMID FROM hive3_tbl_data UNION ALL SELECT COLUMID FROM hive1_tbl_data) t) t
left join
(select distinct t.COLUMID from (SELECT COLUMID FROM hive4_tbl_data UNION ALL SELECT COLUMID FROM hive2_tbl_data) t) t1
on t.COLUMID = t1.COLUMID where t1.COLUMID is null;

select t.COLUMID from 
(select distinct t.COLUMID as COLUMID from (SELECT COLUMID FROM hive3_tbl_data UNION ALL SELECT COLUMID FROM hive1_tbl_data) t) t
left join
(select distinct t.COLUMID from (SELECT COLUMID FROM hive4_tbl_data UNION ALL SELECT COLUMID FROM hive2_tbl_data) t) t1
on t.COLUMID = t1.COLUMID where t1.COLUMID is null;
