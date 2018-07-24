drop table tbl1;

-- tbl1 is only used to create a directory with data
CREATE TABLE tbl1 (index int, value int) LOCATION 'file:${system:test.tmp.dir}/external_insert';
insert into tbl1 VALUES (2, 2);

CREATE external TABLE tbl2 (index int, value int ) PARTITIONED BY ( created_date string );
ALTER TABLE tbl2 ADD PARTITION(created_date='2018-02-01');
ALTER TABLE tbl2 PARTITION(created_date='2018-02-01') SET LOCATION 'file:${system:test.tmp.dir}/external_insert';
select * from tbl2;
describe formatted tbl2 partition(created_date='2018-02-01');
insert into tbl2 partition(created_date='2018-02-01') VALUES (1, 1);
select * from tbl2;
describe formatted tbl2 partition(created_date='2018-02-01');
