set hive.explain.user=true;
set hive.optimize.index.filter=true;
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;

source ${system:hive.root}/data/files/starships.sql;

set hive.optimize.shared.work.dppunion=false;

create table torpedos2 as select * from torpedos;

alter table torpedos2 update statistics set(
'numRows'='12345678',
'rawDataSize'='123456789');


explain
select * from torpedos t,ships s,ship_types st where t.ship_id = s.id and s.ship_type_id=st.id and st.type_name = 'galaxy class'
union
select * from torpedos t,ships s,ship_types st where t.ship_id = s.id and s.ship_type_id=st.id and st.type_name = 'apollo class'
;

set hive.optimize.shared.work.dppunion=true;
