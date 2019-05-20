set hive.mapred.mode=nonstrict;
set hive.auto.convert.join=true;

create table tbl_n1 (n bigint, t string); 

insert into tbl_n1 values (1, 'one'); 
insert into tbl_n1 values(2, 'two');

explain
select a.n, a.t, isnull(b.n), isnull(b.t) from (select * from tbl_n1 where n = 1) a  left outer join  (select * from tbl_n1 where 1 = 2) b on a.n = b.n;
select a.n, a.t, isnull(b.n), isnull(b.t) from (select * from tbl_n1 where n = 1) a  left outer join  (select * from tbl_n1 where 1 = 2) b on a.n = b.n;

explain
select isnull(a.n), isnull(a.t), b.n, b.t from (select * from tbl_n1 where 2 = 1) a  right outer join  (select * from tbl_n1 where n = 2) b on a.n = b.n;
select isnull(a.n), isnull(a.t), b.n, b.t from (select * from tbl_n1 where 2 = 1) a  right outer join  (select * from tbl_n1 where n = 2) b on a.n = b.n;

explain
select isnull(a.n), isnull(a.t), isnull(b.n), isnull(b.t) from (select * from tbl_n1 where n = 1) a  full outer join  (select * from tbl_n1 where n = 2) b on a.n = b.n;
select isnull(a.n), isnull(a.t), isnull(b.n), isnull(b.t) from (select * from tbl_n1 where n = 1) a  full outer join  (select * from tbl_n1 where n = 2) b on a.n = b.n;

explain
select a.key, a.a_one, b.b_one, a.a_zero, b.b_zero from ( SELECT 11 key, 0 confuse_you, 1 a_one, 0 a_zero ) a join ( SELECT 11 key, 0 confuse_you, 1 b_one, 0 b_zero ) b on a.key = b.key;
select a.key, a.a_one, b.b_one, a.a_zero, b.b_zero from ( SELECT 11 key, 0 confuse_you, 1 a_one, 0 a_zero ) a join ( SELECT 11 key, 0 confuse_you, 1 b_one, 0 b_zero ) b on a.key = b.key;

explain
select a.key, a.a_one, b.b_one, a.a_zero, b.b_zero from ( SELECT 11 key, 0 confuse_you, 1 a_one, 0 a_zero ) a left outer join ( SELECT 11 key, 0 confuse_you, 1 b_one, 0 b_zero ) b on a.key = b.key;
select a.key, a.a_one, b.b_one, a.a_zero, b.b_zero from ( SELECT 11 key, 0 confuse_you, 1 a_one, 0 a_zero ) a left outer join ( SELECT 11 key, 0 confuse_you, 1 b_one, 0 b_zero ) b on a.key = b.key;

explain
select a.key, a.a_one, b.b_one, a.a_zero, b.b_zero from ( SELECT 11 key, 0 confuse_you, 1 a_one, 0 a_zero ) a right outer join ( SELECT 11 key, 0 confuse_you, 1 b_one, 0 b_zero ) b on a.key = b.key;
select a.key, a.a_one, b.b_one, a.a_zero, b.b_zero from ( SELECT 11 key, 0 confuse_you, 1 a_one, 0 a_zero ) a right outer join ( SELECT 11 key, 0 confuse_you, 1 b_one, 0 b_zero ) b on a.key = b.key;

explain
select a.key, a.a_one, b.b_one, a.a_zero, b.b_zero from ( SELECT 11 key, 0 confuse_you, 1 a_one, 0 a_zero ) a right outer join ( SELECT 11 key, 0 confuse_you, 1 b_one, 0 b_zero ) b on a.key = b.key;
select a.key, a.a_one, b.b_one, a.a_zero, b.b_zero from ( SELECT 11 key, 0 confuse_you, 1 a_one, 0 a_zero ) a full outer join ( SELECT 11 key, 0 confuse_you, 1 b_one, 0 b_zero ) b on a.key = b.key;
