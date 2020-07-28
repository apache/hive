create table a(k string) partitioned by(hs int);
create table b(k string) partitioned by(hs int);

set hive.cbo.enable=true;
explain extended select * from a where rand(1) < 0.1;

set hive.cbo.enable=false;
explain extended select * from a where rand(1) < 0.1;

set hive.cbo.enable=true;
explain extended select a.*, b.* from a join b on a.k = b.k where a.hs = 11 and rand(1) < 0.1 and b.hs <= 10;

set hive.cbo.enable=false;
explain extended select a.*, b.* from a join b on a.k = b.k where a.hs = 11 and rand(1) < 0.1 and b.hs <= 10;

set hive.cbo.enable=true;
explain extended select * from
(select a.k ak, b.* from a join b on a.k = b.k where rand(1) < 0.1 and a.hs = 11 and b.hs <= 10) x
where length(x.ak) > 3;

set hive.cbo.enable=false;
explain extended select * from
(select a.k ak, b.* from a join b on a.k = b.k where rand(1) < 0.1 and a.hs = 11 and b.hs <= 10) x
where length(x.ak) > 3;
