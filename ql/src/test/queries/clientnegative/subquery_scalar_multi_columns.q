set hive.mapred.mode=nonstrict;
select p_name from part where p_size > (select p_size, p_type from part);
