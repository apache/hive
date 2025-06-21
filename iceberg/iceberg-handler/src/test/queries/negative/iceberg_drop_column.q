-- SORT_QUERY_RESULTS
set hive.cli.print.header=true;

create external table ice_tbl (
  strcol string,
  intcol integer,
  pcol string,
  datecol date
) partitioned by spec (pcol, datecol)
stored by iceberg;

show columns in ice_tbl;

alter table ice_tbl drop column misscol;
