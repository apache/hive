set hive.acid.direct.insert.enabled=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.autogather=false;

drop table if exists io_test_acid_part;
drop table if exists io_test_text_1;
drop table if exists io_test_text_2;
drop table if exists io_test_text_3;
drop table if exists io_test_acid_1;
drop table if exists io_test_acid_2;
drop table if exists io_test_acid_3;

create external table io_test_text_1 (a int, b int, c int) stored as textfile;

insert into io_test_text_1 values (1111, 11, 1111), (2222, 22, 1111), (3333, 33, 2222), (4444, 44, NULL), (5555, 55, NULL);

create external table io_test_text_2 (a int, b int, c int) stored as textfile;

insert into io_test_text_2 values (1111, 11, 1111), (2222, 22, 1111), (3333, 33, 2222), (4444, 44, 4444), (5555, 55, 4444);

-- non-partitioned table

create table io_test_acid (a int, b int, c int) stored as orc tblproperties('transactional'='true');

insert overwrite table io_test_acid select a, b, c from io_test_text_1 where c is not null;

select * from io_test_acid order by a;

insert overwrite table io_test_acid select a, b, c from io_test_text_1 where c is null;

select * from io_test_acid order by a;

insert overwrite table io_test_acid select a, b, c from io_test_text_2 where c is null;

select * from io_test_acid order by a;

insert overwrite table io_test_acid select a, b, c from io_test_text_2 where c is not null;

select * from io_test_acid order by a;

drop table io_test_acid;

create table io_test_acid_1 (a int, b int, c int) stored as orc tblproperties('transactional'='true');
create table io_test_acid_2 (a int, b int, c int) stored as orc tblproperties('transactional'='true');
create table io_test_acid_3 (a int, b int, c int) stored as orc tblproperties('transactional'='true');

from io_test_text_2
insert overwrite table io_test_acid_1 select a, b, c where c=1111
insert overwrite table io_test_acid_2 select a, b, c where c=2222
insert overwrite table io_test_acid_3 select a, b, c where c=4444
;

select * from io_test_acid_1 order by a;
select * from io_test_acid_2 order by a;
select * from io_test_acid_3 order by a;

from io_test_text_1
insert overwrite table io_test_acid_1 select a, b, c where c is null
insert overwrite table io_test_acid_2 select a, b, c where c=7777
insert overwrite table io_test_acid_3 select a, b, c where c is not null
;

select * from io_test_acid_1 order by a;
select * from io_test_acid_2 order by a;
select * from io_test_acid_3 order by a;

drop table io_test_acid_1;
drop table io_test_acid_2;
drop table io_test_acid_3;


-- static partitioning

create table io_test_acid_part (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

insert overwrite table io_test_acid_part partition (c=1) select a, b from io_test_text_1 where c is not null;

select * from io_test_acid_part order by a;

insert overwrite table io_test_acid_part partition (c=2) select a, b from io_test_text_1 where c is null;

select * from io_test_acid_part order by a;

insert overwrite table io_test_acid_part partition (c=1) select a, b from io_test_text_1 where c is null;

select * from io_test_acid_part order by a, c;

insert overwrite table io_test_acid_part partition (c=1) select a, b from io_test_text_2 where c is null;

select * from io_test_acid_part order by a;

insert overwrite table io_test_acid_part partition (c=1) select a, b from io_test_text_2 where c is not null;

select * from io_test_acid_part order by a, c;

drop table io_test_acid_part;

create table io_test_acid_part (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

from io_test_text_2
insert overwrite table io_test_acid_part partition (c=1) select a, b where c=1111
insert overwrite table io_test_acid_part partition (c=2) select a, b where c=2222
insert overwrite table io_test_acid_part partition (c=3) select a, b where c=4444
;

select * from io_test_acid_part order by a;

from io_test_text_1
insert overwrite table io_test_acid_part partition (c=1) select a, b where c is null
insert overwrite table io_test_acid_part partition (c=2) select a, b where c=7777
insert overwrite table io_test_acid_part partition (c=3) select a, b where c is not null
;

select * from io_test_acid_part order by a;

from io_test_text_1
insert overwrite table io_test_acid_part partition (c=1) select a, b where c=8888
insert overwrite table io_test_acid_part partition (c=2) select a, b where c=7777
insert overwrite table io_test_acid_part partition (c=3) select a, b where c=9999
;

select * from io_test_acid_part order by a;

drop table io_test_acid_part;


-- dynamic partitioning

create table io_test_acid_part (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

insert overwrite table io_test_acid_part partition (c) select a, b, c from io_test_text_1 where c is not null;

select * from io_test_acid_part order by a;

insert overwrite table io_test_acid_part partition (c) select a, b, c from io_test_text_1 where c is null;

select * from io_test_acid_part order by a;

insert overwrite table io_test_acid_part partition (c) select a, b, c from io_test_text_2 where b=11 or b=44 or b=99;

select * from io_test_acid_part order by a, c;

insert overwrite table io_test_acid_part partition (c) select a, b, c from io_test_text_2 where b=99;

select * from io_test_acid_part order by a, c;

drop table io_test_acid_part;

create table io_test_acid_part (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

from io_test_text_1
insert overwrite table io_test_acid_part partition (c) select a, b, c where c is not null
insert overwrite table io_test_acid_part partition (c) select a, b, c where c is null
;

select * from io_test_acid_part order by a;

from io_test_text_1
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=11
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=99
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=44
;

select * from io_test_acid_part order by a;

from io_test_text_2
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=7
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=44
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=9
;

select * from io_test_acid_part order by a, c;

drop table io_test_acid_part;

create table io_test_acid_part (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

from io_test_text_2
insert overwrite table io_test_acid_part partition (c) select a, b, c where c is not null
insert overwrite table io_test_acid_part partition (c) select a, b, c where c is null
;

select * from io_test_acid_part order by a;

drop table io_test_acid_part;

create table io_test_acid_part (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

from io_test_text_2
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=11
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=99
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=33
insert overwrite table io_test_acid_part partition (c) select a, b, c where b=88
;

select * from io_test_acid_part order by a;

drop table io_test_acid_part;

set hive.acid.direct.insert.enabled=true;


create external table io_test_text_3 (a int, b int, pc1 int, pc2 int, pc3 int) stored as textfile;

insert into io_test_text_3 values (11,11,11,11,11),
(12,12,11,11,11),
(13,13,11,11,22),
(14,15,11,11,NULL),
(16,16,11,22,11),
(17,17,11,22,22),
(18,18,11,22,NULL),
(19,19,11,22,NULL),
(20,20,22,11,11),
(21,21,22,11,11),
(22,22,22,22,11),
(23,23,22,NUll,11),
(24,24,22,NUll,22),
(25,25,22,NULL,NULL),
(26,26,NULL,11,11),
(27,27,NULL,22,11),
(28,28,NULL,NULL,11),
(29,29,NULL,NULL,22),
(30,30,NULL,NULL,NULL);

create table io_test_acid_part (a int, b int) partitioned by (pc1 int, pc2 int, pc3 int) stored as orc  tblproperties('transactional'='true');

from io_test_text_3
insert overwrite table io_test_acid_part partition (pc1, pc2, pc3)
  select a, b, pc1, pc2, pc3
  where pc1 is not null and pc2 is not null and pc3 is not null
insert overwrite table io_test_acid_part partition (pc1, pc2, pc3)
  select a, b, pc1, pc2, pc3
  where pc2 is null and pc1 is not null
insert overwrite table io_test_acid_part partition (pc1, pc2, pc3)
  select a, b, pc1, pc2, pc3
  where pc3 is null and pc2 is not null and pc1 is not null
insert overwrite table io_test_acid_part partition (pc1, pc2, pc3)
  select a, b, pc1, pc2, pc3
  where pc1 is null
insert overwrite table io_test_acid_part partition (pc1, pc2, pc3)
  select a, b, pc1, pc2, pc3
  where a=111
;

select * from io_test_acid_part order by a;

drop table io_test_acid_part;
drop table io_test_text_1;
drop table io_test_text_2;
drop table io_test_text_3;
