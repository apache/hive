set hive.mapjoin.maxsize=1;
set hive.task.progress=true;

select /*+ mapjoin(b) */ * from src a join src b on (a.key=b.key);
