set hive.merge.nway.joins=true;

create table taba(id string);
create table tabb(id string);
create table tabc(id string);

INSERT INTO TABLE taba VALUES ('1'),('2');
INSERT INTO TABLE tabc VALUES ('1'),('2'),('2');

explain
select * from taba A left outer join tabb B on (A.id = B.id) left outer join tabc C on (C.id = A.id) where B.id is null;
explain cbo
select * from taba A left outer join tabb B on (A.id = B.id) left outer join tabc C on (C.id = A.id) where B.id is null;
select * from taba A left outer join tabb B on (A.id = B.id) left outer join tabc C on (C.id = A.id) where B.id is null;

INSERT INTO TABLE tabb VALUES ('1');

select * from taba A left outer join tabb B on (A.id = B.id) left outer join tabc C on (C.id = A.id) where B.id is null;

INSERT INTO TABLE tabb VALUES ('2');

select * from taba A left outer join tabb B on (A.id = B.id) left outer join tabc C on (C.id = A.id) where B.id is null;
