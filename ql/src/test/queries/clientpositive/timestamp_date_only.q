select cast("2016-12-29 23:59:59" as timestamp) < "2016-12-30";
select cast("2016-12-30 00:00:00" as timestamp) = "2016-12-30";
select cast("2016-12-30 00:00:01" as timestamp) > "2016-12-30";