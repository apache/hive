CREATE TABLE dimension_date (
  id int,
  dateid int,
  completedate string,
  daynumberinweek tinyint,
  dayfullname string,
  daynumberinmonth tinyint,
  daynumberinyear int,
  weeknumberinyear tinyint,
  monthnumberinyear tinyint,
  monthfullname string,
  quarternumber tinyint,
  quartername string,
  yearnumber int,
  weekstartdate string,
  weekstartdateid int,
  monthstartdate string,
  monthstartdateid int);


explain
 with daily as (
 select * 
 from dimension_date
 where dateid = 20200228
 ),
 weekly as (
 select dly.dateid,count(1) as mb_wk
 from dimension_date dly
 left join dimension_date wk
 ON datediff(dly.completedate, wk.completedate) >= 0 
 AND datediff(dly.completedate, wk.completedate) < 6
 GROUP BY dly.dateid
 ),
 monthly as (
 select dly.dateid,count(1) as nb_monthly
 from dimension_date dly
 left join dimension_date wk
 ON datediff(dly.completedate, wk.completedate) >= 0 
 AND datediff(dly.completedate, wk.completedate) < 28
 GROUP BY dly.dateid
 )
 select daily.dateid,mb_wk,nb_monthly
 from daily
 left join weekly on daily.dateid = weekly.dateid
 left join monthly on daily.dateid = monthly.dateid;
