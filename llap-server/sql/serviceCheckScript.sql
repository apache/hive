set hive.execution.mode=llap;
set hive.llap.execution.mode=all;


CREATE temporary TABLE ${hiveconf:hiveLlapServiceCheck} (name VARCHAR(64), age INT)
  CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC;
 
INSERT INTO TABLE ${hiveconf:hiveLlapServiceCheck}
  VALUES ('fred flintstone', 35), ('barney rubble', 32);
 
select count(1) from ${hiveconf:hiveLlapServiceCheck};

