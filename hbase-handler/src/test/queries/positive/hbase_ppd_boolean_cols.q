CREATE TABLE hbase_table(row_key string, c1 boolean, c2 boolean)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,cf:c1,cf:c2"
);
insert into hbase_table
values ('Alex', true, true),
       ('George', true, false),
       ('Stam', false, false),
       ('Alice', false, true),
       ('Chris', null, false),
       ('Rob', false, null);

-- Although we are hitting the code for predicate pushdown the push does not happen at the moment (see HIVE-25939).
explain select * from hbase_table where c1 and c2;
select * from hbase_table where c1 and c2;
explain select * from hbase_table where c1=true and c2=true;
select * from hbase_table where c1=true and c2=true;
