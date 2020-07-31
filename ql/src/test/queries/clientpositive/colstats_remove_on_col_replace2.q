set metastore.colstats.retain.on.column.removal=false;

CREATE TABLE p1(a INT, b INTEGER) PARTITIONED BY(part INT);

insert into table p1 partition (part=1) VALUES (1, 'new');

select 'expected: column stats on a,b';
desc formatted p1 partition (part=1);

alter table p1 replace columns (a int, c string);

select 'expected: no column stats present';
desc formatted p1 partition (part=1);
