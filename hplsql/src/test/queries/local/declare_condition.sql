DECLARE cnt_condition CONDITION;
DECLARE EXIT HANDLER FOR cnt_condition
  PRINT 'Condition raised';  
IF 1 <> 2 THEN
  SIGNAL cnt_condition;
END IF;
PRINT 'Must not be printed 1';
PRINT 'Must not be printed 2';