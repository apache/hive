DROP TABLE src_table;
CREATE TABLE src_table (key int);
LOAD DATA LOCAL INPATH '../../data/files/kv6.txt' INTO TABLE src_table;

-- verify table has data
select assert_true(count(*) = 100) from src_table;
-- should coerce key (int) and 355.8 to be comparable types
select count(*) from src_table where key in (355.8);
-- should coerce key (int) and 1.0 to be comparable types, but 1 exists in the table
select count(*) from src_table where key in (1.0, 0.0);
-- should coerce key (int) and 355.8 to be comparable types
select count(*) from src_table where key not in (355.8);
-- should coerce key (int) and 1.0 to be comparable types, but 1 exists in the table
select count(*) from src_table where key not in (1.0, 0.0);
