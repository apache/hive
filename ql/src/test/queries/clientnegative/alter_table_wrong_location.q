create table testwrongloc(id int);

-- Assume port 12345 is not open
alter table testwrongloc set location "hdfs://localhost:12345/tmp/testwrongloc";
