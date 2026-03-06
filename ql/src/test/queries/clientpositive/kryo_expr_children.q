set hive.cbo.enable=false;

CREATE TABLE tab(attr varchar(5));

-- test case for HIVE-29488
select * from tab t1 left join tab t2
on t1.attr = t2.attr and t2.attr in ( trim(t1.attr), '*');

DROP TABLE tab;
