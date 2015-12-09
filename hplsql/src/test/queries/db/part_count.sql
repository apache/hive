if part_count(partition_date_1) = 5 then
  print 'success';
else 
  print 'failed';
end if;  

if part_count(partition_date_1, region='1') = 2 then
  print 'success';
else 
  print 'failed';
end if;  

if part_count(partition_date_1a) is null then    -- table does not exist  
  print 'success';
else 
  print 'failed';
end if;  
