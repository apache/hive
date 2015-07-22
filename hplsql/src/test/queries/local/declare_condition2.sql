DECLARE cnt_condition CONDITION;
DECLARE CONTINUE HANDLER FOR cnt_condition
  PRINT 'Wrong condition';  
DECLARE CONTINUE HANDLER FOR cnt_condition2
  PRINT 'Condition raised';  
IF 1 <> 2 THEN
  SIGNAL cnt_condition2;
END IF;
PRINT 'Executed 1';
PRINT 'Executed 2';