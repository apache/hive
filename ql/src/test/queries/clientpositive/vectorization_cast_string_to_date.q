CREATE TABLE `test13`(
  `portfolio_valuation_date` string,
  `price_cut_off_datetime` string,
  `portfolio_id_valuation_source` string,
  `contributor_full_path` string,
  `position_market_value` double,
  `mandate_name` string)
STORED AS ORC;

INSERT INTO test13 values (
"2020-01-31",	"2020-02-07T03:14:48.007Z",	"37",	NULL,	-0.26,	"foo");

INSERT INTO test13 values (
"2020-01-31",	"2020-02-07T03:14:48.007Z",	"37",	NULL,	0.33,	"foo");

INSERT INTO test13 values (
"2020-01-31",	"2020-02-07T03:14:48.007Z",	"37",	NULL,	-0.03,	"foo");

INSERT INTO test13 values (
"2020-01-31",	"2020-02-07T03:14:48.007Z",	"37",	NULL,	0.16,	"foo");

INSERT INTO test13 values (
"2020-01-31",	"2020-02-07T03:14:48.007Z",	"37",	NULL,	0.08,	"foo");

set hive.fetch.task.conversion=none;
set hive.explain.user=false;

set hive.vectorized.execution.enabled=false;
select Cast(`test13`.`price_cut_off_datetime` AS date) from test13;


set hive.vectorized.execution.enabled=true;
select Cast(`test13`.`price_cut_off_datetime` AS date) from test13;
