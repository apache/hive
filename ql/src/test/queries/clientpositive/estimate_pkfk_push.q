source ${system:hive.root}/data/files/starships.sql;

-- conditions on the join key from one side ; is pushed to the other side to reduce computation
-- however: we should not use the ratio twice (and loose accuracy)
-- this was the primary issue of HIVE-22238

set hive.explain.user=true;
set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.hooks.AccurateEstimatesCheckerHook;

explain analyze
select
    s.id
from
    ships s,
    ship_types st,
    torpedos t
where
    (st.id = 1 or st.id=2)
    and ship_type_id=st.id
    and ship_id=s.id
;
