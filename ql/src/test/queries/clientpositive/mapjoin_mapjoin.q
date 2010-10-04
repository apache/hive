explain select /*+MAPJOIN(src, src1) */ srcpart.key from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key);

explain select /*+MAPJOIN(src, src1) */ count(*) from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key) group by ds;

select /*+MAPJOIN(src, src1) */ count(*) from srcpart join src src on (srcpart.value=src.value) join src src1 on (srcpart.key=src1.key) group by ds;
