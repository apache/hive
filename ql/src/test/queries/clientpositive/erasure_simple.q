--! qt:dataset:src

-- Test Erasure Coding Policies

ERASURE echo listPolicies originally was;
ERASURE listPolicies;
ERASURE enablePolicy --policy RS-10-4-1024k;
ERASURE enablePolicy --policy XOR-2-1-1024k;
ERASURE echo listPolicies after enablePolicy;
ERASURE listPolicies;

dfs ${system:test.dfs.mkdir} hdfs:///tmp/erasure_coding1;

ERASURE echo original policy on erasure_coding1;
ERASURE getPolicy --path hdfs:///tmp/erasure_coding1;

ERASURE echo set the default policy on erasure_coding1;
ERASURE setPolicy --path hdfs:///tmp/erasure_coding1 --policy RS-10-4-1024k;

ERASURE echo new policy on erasure_coding1;
ERASURE getPolicy --path hdfs:///tmp/erasure_coding1;

ERASURE echo unset the default policy on erasure_coding1;
ERASURE unsetPolicy --path hdfs:///tmp/erasure_coding1;
ERASURE getPolicy --path hdfs:///tmp/erasure_coding1;

create table erasure_table (a int) location 'hdfs:///tmp/erasure_coding1/location1';

-- insert some data with the default policy (RS-3-2-1024k) from the fs root
insert into erasure_table values(4);


-- set a new policy on the directory and insert some data
ERASURE setPolicy --path hdfs:///tmp/erasure_coding1 --policy XOR-2-1-1024k;
insert into erasure_table values(5);

ERASURE echo policy on older file is;
ERASURE getPolicy --path hdfs:///tmp/erasure_coding1/location1/000000_0;
ERASURE echo policy on newer file is;
ERASURE getPolicy --path hdfs:///tmp/erasure_coding1/location1/000000_0_copy_1;

-- show that data is present
select * from erasure_table;

drop table if exists erasure_table2;
create table erasure_table2 like src  location 'hdfs:///tmp/erasure_coding1/location2';
insert overwrite table erasure_table2
select key, value from src;

ERASURE echo show table extended like erasure_table2;
show table extended like erasure_table2;

ERASURE echo SHOW TBLPROPERTIES erasure_table2;
SHOW TBLPROPERTIES erasure_table2;

ERASURE echo unset the default policy on erasure_coding1;
ERASURE unsetPolicy --path hdfs:///tmp/erasure_coding1;

dfs -rmr hdfs:///tmp/erasure_coding1;

ERASURE echo disablePolicy  RS-10-4-1024k;
ERASURE disablePolicy --policy  RS-10-4-1024k;

ERASURE echo disablePolicy  XOR-2-1-1024k;
ERASURE disablePolicy --policy  XOR-2-1-1024k;
