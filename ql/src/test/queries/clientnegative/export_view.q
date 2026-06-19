create table t ( a int );

create view export_view as select * from t;

export table export_view to 'ql/test/data/exports/export_view';
