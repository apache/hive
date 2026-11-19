-- The position deletes of a V2 merge-on-read DELETE, split UPDATE or MERGE are not sorted when the fanout writer is
-- disabled: the delete writer buffers the positions of each data file either way, so a sort before it is only cost.
set hive.explain.user=false;

create external table ice_v2_nofanout (id int, name string, amount int) partitioned by spec (bucket(2, id))
stored by iceberg tblproperties ('format-version'='2', 'write.fanout.enabled'='false',
  'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read');
insert into ice_v2_nofanout values (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40);

create external table ice_v2_src (id int) stored by iceberg tblproperties ('format-version'='2');
insert into ice_v2_src values (3), (4), (5);

-- no reducer
explain delete from ice_v2_nofanout where id = 1;
delete from ice_v2_nofanout where id = 1;

-- no reducer: the inserted rows of one partition are not sorted
explain update ice_v2_nofanout set amount = -1 where id = 2;
update ice_v2_nofanout set amount = -1 where id = 2;

merge into ice_v2_nofanout t using ice_v2_src s on t.id = s.id
when matched and t.id = 3 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 0);

-- (2, 'b', -1), (3, 'mrg', 30), (5, 'new', 0)
select * from ice_v2_nofanout order by id, name;
