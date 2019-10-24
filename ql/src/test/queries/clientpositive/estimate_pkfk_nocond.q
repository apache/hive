
--! qt:dataset:starships

-- conditions on one side of the join ; PK/FK scale should be used

-- set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.hooks.AccurateEstimatesCheckerHook;


explain analyze
select
    'expected: 20 rows from first join and 200 after second',
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
