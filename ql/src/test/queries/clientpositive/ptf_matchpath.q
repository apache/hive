set hive.vectorized.execution.enabled=false;
set hive.explain.user=false;

DROP TABLE flights_tiny;

create table flights_tiny ( 
ORIGIN_CITY_NAME string, 
DEST_CITY_NAME string, 
YEAR int, 
MONTH int, 
DAY_OF_MONTH int, 
ARR_DELAY float, 
FL_NUM string 
);

LOAD DATA LOCAL INPATH '../../data/files/flights_tiny.txt' OVERWRITE INTO TABLE flights_tiny;

-- SORT_QUERY_RESULTS

-- 1. basic Matchpath test
explain
select origin_city_name, fl_num, year, month, day_of_month, sz, tpath 
from matchpath(on 
        flights_tiny 
        distribute by fl_num 
        sort by year, month, day_of_month  
      arg1('LATE.LATE+'), 
      arg2('LATE'), arg3(arr_delay > 15), 
    arg4('origin_city_name, fl_num, year, month, day_of_month, size(tpath) as sz, tpath[0].day_of_month as tpath') 
   );       

select origin_city_name, fl_num, year, month, day_of_month, sz, tpath 
from matchpath(on 
        flights_tiny 
        distribute by fl_num 
        sort by year, month, day_of_month  
      arg1('LATE.LATE+'), 
      arg2('LATE'), arg3(arr_delay > 15), 
    arg4('origin_city_name, fl_num, year, month, day_of_month, size(tpath) as sz, tpath[0].day_of_month as tpath') 
   );       

-- 2. Matchpath on 1 partition
explain
select origin_city_name, fl_num, year, month, day_of_month, sz, tpath 
from matchpath(on 
        flights_tiny 
        sort by fl_num, year, month, day_of_month  
      arg1('LATE.LATE+'), 
      arg2('LATE'), arg3(arr_delay > 15), 
    arg4('origin_city_name, fl_num, year, month, day_of_month, size(tpath) as sz, tpath[0].day_of_month as tpath') 
   )
where fl_num = 1142;

select origin_city_name, fl_num, year, month, day_of_month, sz, tpath 
from matchpath(on 
        flights_tiny 
        sort by fl_num, year, month, day_of_month  
      arg1('LATE.LATE+'), 
      arg2('LATE'), arg3(arr_delay > 15), 
    arg4('origin_city_name, fl_num, year, month, day_of_month, size(tpath) as sz, tpath[0].day_of_month as tpath') 
   )
where fl_num = 1142;

-- 3. empty partition.
explain
select origin_city_name, fl_num, year, month, day_of_month, sz, tpath
from matchpath(on
        (select * from flights_tiny where fl_num = -1142) flights_tiny
        sort by fl_num, year, month, day_of_month
      arg1('LATE.LATE+'),
      arg2('LATE'), arg3(arr_delay > 15),
    arg4('origin_city_name, fl_num, year, month, day_of_month, size(tpath) as sz, tpath[0].day_of_month as tpath')
   );
   

select origin_city_name, fl_num, year, month, day_of_month, sz, tpath
from matchpath(on
        (select * from flights_tiny where fl_num = -1142) flights_tiny
        sort by fl_num, year, month, day_of_month
      arg1('LATE.LATE+'),
      arg2('LATE'), arg3(arr_delay > 15),
    arg4('origin_city_name, fl_num, year, month, day_of_month, size(tpath) as sz, tpath[0].day_of_month as tpath')
   );
   
