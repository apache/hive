!mkdir ${system:test.tmp.dir}/tmpjars;
!touch ${system:test.tmp.dir}/tmpjars/added1.jar;
!touch ${system:test.tmp.dir}/tmpjars/added2.jar;

select count(key) from src;

add jar ${system:test.tmp.dir}/tmpjars/added1.jar;
add jar ${system:test.tmp.dir}/tmpjars/added2.jar;

select count(key) from src;

!rm ${system:test.tmp.dir}/tmpjars/added1.jar;

select count(key) from src;

