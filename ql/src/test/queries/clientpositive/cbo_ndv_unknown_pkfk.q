--! qt:dataset:src
--! qt:replace:/rowcount = ([0-9.E]+), cumulative cost = \{.*\}, id = [0-9]+/rowcount: $1/

-- HIVE-29625: PK-FK join estimates from RELY constraints when an NDV is unknown (binary keys), followed by
-- string-key cases with known NDVs for comparison. Each estimate is followed by the actual row count.

set hive.stats.column.autogather=false;
create table pkfk_pk (k binary, v string) stored as orc;
create table pkfk_fk (k binary, v string) stored as orc;
alter table pkfk_pk add constraint pkfk_pk_pk primary key (k) disable novalidate rely;
alter table pkfk_fk add constraint pkfk_fk_fk foreign key (k) references pkfk_pk(k) disable novalidate rely;
insert into pkfk_pk select cast(value as binary), max(key) from src group by value;
insert into pkfk_fk select cast(value as binary), key from src;
-- Only the foreign key side has column statistics: FK NDV unknown, PK NDV estimated
analyze table pkfk_fk compute statistics for columns;

-- Inner join, the key filter is copied to the foreign key side
explain cbo joincost select p.v, f.v from pkfk_pk p join pkfk_fk f on p.k = f.k where p.k > cast('val_3' as binary);
select count(*) from pkfk_pk p join pkfk_fk f on p.k = f.k where p.k > cast('val_3' as binary);

-- The same inner join with transitive inference off; the key filter is still copied to the foreign key side
set hive.ppd.recognizetransivity=false;
explain cbo joincost select p.v, f.v from pkfk_pk p join pkfk_fk f on p.k = f.k where p.k > cast('val_3' as binary);
select count(*) from pkfk_pk p join pkfk_fk f on p.k = f.k where p.k > cast('val_3' as binary);
set hive.ppd.recognizetransivity=true;

-- Left outer join that keeps every foreign key row, key filter on the primary key side
explain cbo joincost select p.v, f.v from pkfk_fk f left outer join pkfk_pk p
on f.k = p.k and p.k > cast('val_3' as binary);
select count(*) from pkfk_fk f left outer join pkfk_pk p on f.k = p.k and p.k > cast('val_3' as binary);

-- Right outer join that keeps every foreign key row, key filter on the primary key side
explain cbo joincost select p.v, f.v from pkfk_pk p right outer join pkfk_fk f
on p.k = f.k and p.k > cast('val_3' as binary);
select count(*) from pkfk_pk p right outer join pkfk_fk f on p.k = f.k and p.k > cast('val_3' as binary);

-- Semi join with the key filter
explain cbo joincost select f.v from pkfk_fk f where f.k in (select p.k from pkfk_pk p where p.k > cast('val_3' as binary));
select count(*) from pkfk_fk f where f.k in (select p.k from pkfk_pk p where p.k > cast('val_3' as binary));

-- Anti join with the key filter
explain cbo joincost select f.v from pkfk_fk f
where not exists (select 1 from pkfk_pk p where p.k = f.k and p.k > cast('val_3' as binary));
select count(*) from pkfk_fk f
where not exists (select 1 from pkfk_pk p where p.k = f.k and p.k > cast('val_3' as binary));

-- Only the primary key side has column statistics: PK NDV unknown, FK NDV estimated
create table pkfk_pk2 (k binary, v string) stored as orc;
create table pkfk_fk2 (k binary, v string) stored as orc;
alter table pkfk_pk2 add constraint pkfk_pk2_pk primary key (k) disable novalidate rely;
alter table pkfk_fk2 add constraint pkfk_fk2_fk foreign key (k) references pkfk_pk2(k) disable novalidate rely;
insert into pkfk_pk2 select cast(value as binary), max(key) from src group by value;
insert into pkfk_fk2 select cast(value as binary), key from src;
analyze table pkfk_pk2 compute statistics for columns;
explain cbo joincost select p.v, f.v from pkfk_pk2 p join pkfk_fk2 f on p.k = f.k where p.k > cast('val_3' as binary);
select count(*) from pkfk_pk2 p join pkfk_fk2 f on p.k = f.k where p.k > cast('val_3' as binary);

-- Both sides have column statistics: PK and FK NDVs unknown, primary key side filtered on its key
create table pkfk_pk3 (k binary, v string) stored as orc;
create table pkfk_fk3 (k binary, v string) stored as orc;
alter table pkfk_pk3 add constraint pkfk_pk3_pk primary key (k) disable novalidate rely;
alter table pkfk_fk3 add constraint pkfk_fk3_fk foreign key (k) references pkfk_pk3(k) disable novalidate rely;
insert into pkfk_pk3 select cast(value as binary), max(key) from src group by value;
insert into pkfk_fk3 select cast(value as binary), key from src;
analyze table pkfk_pk3 compute statistics for columns;
analyze table pkfk_fk3 compute statistics for columns;
explain cbo joincost select p.v, f.v from pkfk_pk3 p join pkfk_fk3 f on p.k = f.k where p.k > cast('val_3' as binary);
select count(*) from pkfk_pk3 p join pkfk_fk3 f on p.k = f.k where p.k > cast('val_3' as binary);

-- Known NDVs on both sides (string keys), primary key side filtered on its key
create table pkfk_pk4 (k string, v string) stored as orc;
create table pkfk_fk4 (k string, v string) stored as orc;
alter table pkfk_pk4 add constraint pkfk_pk4_pk primary key (k) disable novalidate rely;
alter table pkfk_fk4 add constraint pkfk_fk4_fk foreign key (k) references pkfk_pk4(k) disable novalidate rely;
insert into pkfk_pk4 select value, max(key) from src group by value;
insert into pkfk_fk4 select value, key from src;
analyze table pkfk_pk4 compute statistics for columns;
analyze table pkfk_fk4 compute statistics for columns;
explain cbo joincost select p.v, f.v from pkfk_pk4 p join pkfk_fk4 f on p.k = f.k where p.k > 'val_3';
select count(*) from pkfk_pk4 p join pkfk_fk4 f on p.k = f.k where p.k > 'val_3';

-- The same join with the primary key side filtered on a non-key column: no NDV scaling
explain cbo joincost select p.v, f.v from pkfk_pk4 p join pkfk_fk4 f on p.k = f.k where p.v > '100';
select count(*) from pkfk_pk4 p join pkfk_fk4 f on p.k = f.k where p.v > '100';

-- Left outer join that keeps every foreign key row, primary key side filtered on a non-key column; the estimate
-- applies the primary key side's filter selectivity (pre-existing on master, follow-up)
explain cbo joincost select p.v, f.v from pkfk_fk4 f left outer join pkfk_pk4 p on f.k = p.k and p.v > '100';
select count(*) from pkfk_fk4 f left outer join pkfk_pk4 p on f.k = p.k and p.v > '100';
