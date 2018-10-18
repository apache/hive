create table stats_analyze_decimal_compare (a decimal) tblproperties ("transactional"="false");
insert into stats_analyze_decimal_compare values (5);
insert into stats_analyze_decimal_compare values (10);
desc formatted stats_analyze_decimal_compare a;
