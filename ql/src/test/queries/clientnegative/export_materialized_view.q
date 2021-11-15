create table t ( a int );

create materialized view export_materialized_view disable rewrite as select * from t;

export table export_materialized_view to 'ql/test/data/exports/export_materialized_view';
