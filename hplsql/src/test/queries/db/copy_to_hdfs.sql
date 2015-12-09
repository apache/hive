--copy src to hdfs src.txt;
copy (select * from src) to hdfs /user/hplsql/src2.txt delimiter '\01';
