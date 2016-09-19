create materialized view dmv_mat_view as select cint, cstring1 from alltypesorc where cint < 0;

show table extended like dmv_mat_view;

drop materialized view dmv_mat_view;

show table extended like dmv_mat_view;
