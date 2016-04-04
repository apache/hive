for item in (select code from sample_07 limit 10  ) loop print(cast(item.code as varchar2(100))+' aa') end loop;
