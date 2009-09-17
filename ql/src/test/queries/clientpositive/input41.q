set hive.mapred.mode=strict;

select * from 
  (select count(1) from src 
    union all
   select count(1) from srcpart where ds = '2009-08-09'
  )x;
   

select * from 
  (select * from src 
    union all
   select * from srcpart where ds = '2009-08-09'
  )x;
