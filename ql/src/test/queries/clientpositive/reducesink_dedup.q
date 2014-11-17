select p_name 
from (select p_name from part distribute by 1 sort by 1) p 
distribute by 1 sort by 1
;