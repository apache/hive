create table authorization_part_fail (key int, value string) partitioned by (ds string);
revoke `ALL` on table authorization_part_fail from user hive_test_user;
set hive.security.authorization.enabled=true;

ALTER TABLE authorization_part_fail SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="TRUE");
