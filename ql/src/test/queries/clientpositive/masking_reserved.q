set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table keyword_test_off (id int, `etad` string, key int);
create table keyword_test_on (id int, `date` string, key int);
create table masking_test_n_masking_reserved (id int, value string, key int);

explain select a.`etad`, b.value from keyword_test_off a join masking_test_n_masking_reserved b on b.id = a.id;
select a.`etad`, b.value from keyword_test_off a join masking_test_n_masking_reserved b on b.id = a.id;

explain select a.`date`, b.value from keyword_test_on a join masking_test_n_masking_reserved b on b.id = a.id;
select a.`date`, b.value from keyword_test_on a join masking_test_n_masking_reserved b on b.id = a.id;
