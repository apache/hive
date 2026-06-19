PRINT CASE 1 
        WHEN 0 THEN 'FAILED'
        WHEN 1 THEN 'Correct' 
        WHEN 2 THEN 'FAILED'
        ELSE 'FAILED'
      END 

PRINT CASE 3 
        WHEN 0 THEN 'FAILED'
        WHEN 1 THEN 'FAILED'
        ELSE 'Correct'
      END       
      
PRINT NVL2(CASE 3 
        WHEN 0 THEN 'FAILED'
        WHEN 1 THEN 'FAILED'
      END, 'FAILED', 'Correct')  
      
PRINT CASE  
        WHEN 1=0 THEN 'FAILED'
        WHEN 1=1 THEN 'Correct' 
        WHEN 1=2 THEN 'FAILED'
        ELSE 'FAILED'
      END 

PRINT CASE  
        WHEN 3=0 THEN 'FAILED'
        WHEN 3=1 THEN 'FAILED'
        ELSE 'Correct'
      END       
      
PRINT NVL2(CASE  
        WHEN 3=0 THEN 'FAILED'
        WHEN 3=1 THEN 'FAILED'
      END, 'FAILED', 'Correct') 