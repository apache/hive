from src
select transform('aa\;') using '/bin/cat' as a  limit 1;

from src
select transform('bb') using '/bin/cat' as b limit 1; from src
select transform('cc') using '/bin/cat' as c limit 1;