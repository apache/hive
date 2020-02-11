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

-- grant select, delete privileges to user2 on exchange_partition_test_1

grant select, delete on exchange_partition_test_1 to user user2;

set role all;

set user.name=user2;

show grant user user2 on table exchange_partition_test_1;

-- switch user

-- create a table owned by user2 (as a result user2 will have insert privilege)

create table exchange_partition_test_2(a int) partitioned by (b int);

show grant user user2 on table exchange_partition_test_2;

-- execute alter table exchange partition to add data to exchange_partition_test_2

explain authorization alter table exchange_partition_test_2 exchange partition (b=1) with table exchange_partition_test_1;

alter table exchange_partition_test_2 exchange partition (b=1) with table exchange_partition_test_1;
