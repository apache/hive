insert overwrite directory /tmp/src1
  select * from src;
  
insert overwrite local directory /tmp/src2
  select * from src;
  
insert overwrite local directory '/tmp/src3'
  'select * from ' || 'src';
  
declare tabname string = 'src';
insert overwrite local directory '/tmp/src_' || date '2016-03-28' 
  'select * from ' || tabname;