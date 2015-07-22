create table L as select 4436 id;
create table LA as select 4436 loan_id, 4748 aid, 4415 pi_id;
create table FR as select 4436 loan_id;
create table A as select 4748 id;
create table PI as select 4415 id;

create table acct as select 4748 aid, 10 acc_n, 122 brn;
insert into table acct values(4748, null, null);
insert into table acct values(4748, null, null);

--[HIVE-10841] (WHERE col is not null) does not work sometimes for queries with many JOIN statements
explain select
  acct.ACC_N,
  acct.brn
FROM L
JOIN LA ON L.id = LA.loan_id
JOIN FR ON L.id = FR.loan_id
JOIN A ON LA.aid = A.id
JOIN PI ON PI.id = LA.pi_id
JOIN acct ON A.id = acct.aid
WHERE
  L.id = 4436
  and acct.brn is not null;

select
  acct.ACC_N,
  acct.brn
FROM L
JOIN LA ON L.id = LA.loan_id
JOIN FR ON L.id = FR.loan_id
JOIN A ON LA.aid = A.id
JOIN PI ON PI.id = LA.pi_id
JOIN acct ON A.id = acct.aid
WHERE
  L.id = 4436
  and acct.brn is not null;

set hive.cbo.enable=false;

create table taba(
a01 string,
a02 string,
a03 string);

create table tabb(
b01 string);

create table tabc(
c01 string,
c02 string);

create table tabd(
d01 string);

explain
select a01, * from
(select a01, a02, a03 from taba) ta

join
(select b01 from tabb) tb
on (ta.a02=tb.b01)

join tabc tc
on (tb.b01=tc.c01)

left outer join
(select d01 from tabd ) td
on (td.d01 = tc.c02);

explain select a01, * from
(select a01, a02, a03 from taba) ta

join
(select b01 from tabb) tb
on (ta.a02=tb.b01)

join tabc tc
on (tb.b01=tc.c01)

join
(select d01 from tabd ) td
on (td.d01 = tc.c02);
