-- With the fanout writer disabled, the position deletes of a V2 merge-on-read DELETE, split UPDATE or MERGE are sorted
-- by spec, partition, file and position, and the clustered position delete writer streams them, as the clustered data
-- writer does the inserted rows. It takes each partition once, so the sort has to cluster the partitions exactly.
set hive.explain.user=false;

create external table ice_v2_nofanout (id int, name string, amount int) partitioned by spec (bucket(2, id))
stored by iceberg tblproperties ('format-version'='2', 'write.fanout.enabled'='false',
  'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read');
insert into ice_v2_nofanout values (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40);

create external table ice_v2_src (id int) stored by iceberg tblproperties ('format-version'='2');
insert into ice_v2_src values (3), (4), (5);

explain delete from ice_v2_nofanout where id = 1;
delete from ice_v2_nofanout where id = 1;

explain update ice_v2_nofanout set amount = -1 where id = 2;
update ice_v2_nofanout set amount = -1 where id = 2;

explain merge into ice_v2_nofanout t using ice_v2_src s on t.id = s.id
when matched and t.id = 3 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 0);
merge into ice_v2_nofanout t using ice_v2_src s on t.id = s.id
when matched and t.id = 3 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 0);

-- (2, 'b', -1), (3, 'mrg', 30), (5, 'new', 0)
select * from ice_v2_nofanout order by id, name;

-- partitions (1, 31) and (2, 0) hashed alike with the former PARTITION__HASH, and object storage paths interleave their
-- files: the deletes reach the clustered writer partition by partition all the same
create external table ice_v2_collide (id int, a int, b int) partitioned by spec (a, b)
stored by iceberg tblproperties ('format-version'='2', 'write.fanout.enabled'='false',
  'write.object-storage.enabled'='true');
insert into ice_v2_collide values (1, 1, 31), (2, 2, 0), (3, 1, 31), (4, 2, 0);
insert into ice_v2_collide values (5, 1, 31), (6, 2, 0), (7, 1, 31), (8, 2, 0);
insert into ice_v2_collide values (9, 1, 31), (10, 2, 0);

delete from ice_v2_collide where id % 2 = 1 or id in (4, 8);

-- (2, 2, 0), (6, 2, 0), (10, 2, 0)
select * from ice_v2_collide order by id;
