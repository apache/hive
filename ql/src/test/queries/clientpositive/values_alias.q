set hive.cli.print.header=true;

EXPLAIN CBO
VALUES(1,2 b,3 c),(4,5,6),(11,12,13);

VALUES(1,2 b,3 c),(4,5,6),(11,12,13);

VALUES(1 as a,2 b,3 as c),(4,5,6),(11,12,13);

VALUES(1 a,2 b,3),(4,5,6),(11,12,13)
UNION ALL
VALUES(100 f,200 g,300 h),(400,500,600),(110,120,130);

set hive.cbo.enable=false;

VALUES(1,2 b,3 c),(4,5,6),(11,12,13);

