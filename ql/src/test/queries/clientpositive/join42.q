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
