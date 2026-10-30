-- A V3 table gets one DV per data file: the deleted records are distributed by FILE__PATH
set hive.explain.user=false;

create external table ice_v3 (id int, name string) partitioned by spec (bucket(2, id))
stored by iceberg tblproperties ('format-version'='3');
insert into ice_v3 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

create external table ice_v3_src (id int) stored by iceberg tblproperties ('format-version'='3');
insert into ice_v3_src values (1), (2), (5);

explain delete from ice_v3 where id = 1;
delete from ice_v3 where id = 1;

explain update ice_v3 set name = 'x' where id = 2;
update ice_v3 set name = 'x' where id = 2;

explain merge into ice_v3 t using ice_v3_src s on t.id = s.id
when matched and t.id = 2 then update set name = 'y'
when matched then delete
when not matched then insert values (s.id, 'z');
merge into ice_v3 t using ice_v3_src s on t.id = s.id
when matched and t.id = 2 then update set name = 'y'
when matched then delete
when not matched then insert values (s.id, 'z');

select * from ice_v3 order by id;
