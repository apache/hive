source ${system:hive.root}/data/files/starships.sql;

-- conditions on both side of the join ; but not on the FK ; PK/FK scale should be used

set hive.explain.user=true;

--for this case this can't be enabled because min is used during join selectivity estimation - so the 2. join estimation wont be that good
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


-- but this simpler case can be asserted
set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.hooks.AccurateEstimatesCheckerHook;

explain analyze
select
    s.id
from
    ships s,
    ship_types st
where
    st.type_name='galaxy class' 
    and s.crew_size=2
    and ship_type_id=st.id
;
