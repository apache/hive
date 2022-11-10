--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
set hive.stats.column.autogather=false;
set hive.exec.mode.local.auto=false;
set mapred.reduce.tasks = 10;
set hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=325;

-- This test sets number of mapred tasks to 10 for a database with 50 buckets,
-- and uses a post-hook to confirm that 10 tasks were created

CREATE external TABLE tbl_src1(key int, value string, fld1 string) stored as TEXTFILE;
insert into tbl_src1 values (1,'value11', '2022-01-16 00:00:00.0');
insert into tbl_src1 values (1,'value12', '2022-01-16 15:10:10.1');
insert into tbl_src1 values (1,'value13', '2022-01-16 15:10:10.1');
insert into tbl_src1 values (1,'value14', '2022-01-16 15:10:10.1');
insert into tbl_src1 values (1,'value1', '2022-01-16 15:10:10.1');
insert into tbl_src1 values (1,'value1', '2022-01-16 15:10:10.1');
insert into tbl_src1 values (1,'value1', '2022-01-16 15:10:10.1');
insert into tbl_src1 values (1,'value1', '2022-01-16 15:10:10.1');

CREATE external TABLE tbl_src2(key int, value string, fld1 string) stored as TEXTFILE;
insert into tbl_src2 values (1,'value11', '2022-01-16 00:00:00.0');
insert into tbl_src2 values (1,'value12', '2022-01-16 15:10:10.1');
insert into tbl_src2 values (1,'value13', '2022-01-16 15:10:10.1');
insert into tbl_src2 values (1,'value14', '2022-01-16 15:10:10.1');
insert into tbl_src2 values (1,'value1', '2022-01-16 15:10:10.1');
insert into tbl_src2 values (1,'value1', '2022-01-16 15:10:10.1');
insert into tbl_src2 values (1,'value1', '2022-01-16 15:10:10.1');
insert into tbl_src2 values (1,'value1', '2022-01-16 15:10:10.1');

CREATE external TABLE tbl_dest1 (key int, value string) partitioned by (fld1 string, fld2 int)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

insert into table tbl_dest1 partition (fld1, fld2)
select max(key), value, max(fld1), max(key) from tbl_src1 group by value
union all
select max(key), value, max(fld1), max(key) from tbl_src2 group by value;

select * from tbl_dest1 order  by value;

insert into table tbl_dest1 partition (fld1, fld2)
select key, value, fld1, key from tbl_src2
union all
select key, value, fld1, key from tbl_src1;

select * from tbl_dest1 order  by value;

CREATE external TABLE tbl_dest2 (key int, value string) partitioned by (fld1 string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

insert into table tbl_dest2 partition (fld1)
select key, value, fld1 from tbl_dest1
union all
select key, value, fld1 from tbl_dest1;

select * from tbl_dest2 order  by value;