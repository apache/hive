-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/

--Null case -> if partitions spec is altered. Null partitions need to be ignored.
create table ice2 (a string, b int, c int) PARTITIONED BY (d_part int, e_part int) stored by iceberg stored as orc TBLPROPERTIES("format-version"='2') ;
select * from default.ice2.partitions order by `partition`;
show partitions ice2;


