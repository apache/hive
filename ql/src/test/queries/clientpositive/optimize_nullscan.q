-- SORT_BEFORE_DIFF

explain extended 
select key from src where false;
select key from src where false;

explain extended
select * from (select key from src where false) a left outer join (select key from srcpart limit 0) b on a.key=b.key;
select * from (select key from src where false) a left outer join (select key from srcpart limit 0) b on a.key=b.key;

explain extended
select count(key) from src where false union all select count(key) from srcpart ;
select count(key) from src where false union all select count(key) from srcpart ;

explain extended
select * from (select key from src where false) a left outer join (select value from srcpart limit 0) b ;
select * from (select key from src where false) a left outer join (select value from srcpart limit 0) b ;

explain extended 
select * from (select key from src union all select src.key from src left outer join srcpart on src.key = srcpart.key) a  where false;
select * from (select key from src union all select src.key from src left outer join srcpart on src.key = srcpart.key) a  where false;
