--! qt:authorizer
--! qt:dataset:src

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

-- create a table owned by user2

create table exchange_partition_test_2(a int) partitioned by (b int);

show grant user user2 on table exchange_partition_test_2;

set user.name=user1;

-- switch user

show grant user user1 on table exchange_partition_test_2;

-- execute alter table exchange partition to add data to exchange_partition_test_2 (this should fail)

 explain authorization alter table exchange_partition_test_2 exchange partition (b=1) with table exchange_partition_test_1;
 
 alter table exchange_partition_test_2 exchange partition (b=1) with table exchange_partition_test_1;
