source ../../data/files/starships.sql;

-- conditions on one side of the join ; PK/FK scale should be used

SET hive.vectorized.execution.enabled=false;
set hive.explain.user=true;
-- set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.hooks.AccurateEstimatesCheckerHook;
explain 
select 1 ;


explain analyze
select
    s.id
from
    ships s,
    ship_types st,
    torpedos t
where
    st.type_name='galaxy class' 
    and ship_type_id=st.id
    and ship_id=s.id
;
