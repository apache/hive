set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

set user.name=user1;

-- create a table owned by user1

create table exchange_partition_test_1(a int) partitioned by (b int);

show grant user user1 on table exchange_partition_test_1;

set user.name=hive_admin_user;

set role admin;

-- add data to exchange_partition_test_1

insert overwrite table exchange_partition_test_1 partition (b=1) select key from src;

set role all;

set user.name=user2;

-- switch user

-- create a table owned by user2 (as a result user2 will have insert privilege on exchange_partition_test_2 but no select + delete previlege on exchange_partition_test_1)

create table exchange_partition_test_2(a int) partitioned by (b int);

show grant user user2 on table exchange_partition_test_2;


-- execute alter table exchange partition to add data to exchange_partition_test_2 (this should fail)

 explain authorization alter table exchange_partition_test_2 exchange partition (b=1) with table exchange_partition_test_1;
 
 alter table exchange_partition_test_2 exchange partition (b=1) with table exchange_partition_test_1;
 
 set hive.security.authorization.enabled=false;



