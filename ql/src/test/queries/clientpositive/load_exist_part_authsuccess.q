set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
create table hive_test_src_n1 ( col1 string ) partitioned by (pcol1 string) stored as textfile;
alter table hive_test_src_n1 add partition (pcol1 = 'test_part');
set hive.security.authorization.enabled=true;
grant Update on table hive_test_src_n1 to user hive_test_user;
load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src_n1 partition (pcol1 = 'test_part');
