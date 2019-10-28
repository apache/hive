--set hive.optimize.ppd=false;
set hive.explain.user=true;
--set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.hooks.AccurateEstimatesCheckerHook;


explain analyze
select
    s.id
from
    ships s,
    ship_types st,
    torpedos t
where
    st.type_name='galaxy class' 
    and s.crew_size=2
    and ship_type_id=st.id
    and ship_id=s.id
;
