copy src to target/tmp/src.txt;
copy (select * from src) to target/tmp/src2.txt sqlinsert src2;