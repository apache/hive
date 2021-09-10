set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE table_check_merge (
    name string,
    age int,
    gpa double CHECK (gpa BETWEEN 0.0 AND 4.0)
) stored as orc TBLPROPERTIES ('transactional'='true');

CREATE TABLE table_source(
    name string,
    age int,
    gpa double
);

insert into table_source(name, age, gpa) values
('student1', 16, null),
(null, 20, 4.0);

insert into table_check_merge(name, age, gpa) values
('student1', 16, 2.0);

-- This should fail since gpa=6 fails gpa BETWEEN 0.0 AND 4.0
merge into table_check_merge using (select age from table_source)source
on source.age=table_check_merge.age
when matched then update set gpa=6;
