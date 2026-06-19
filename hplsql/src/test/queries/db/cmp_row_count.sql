cmp row_count src, src at hive2conn;
cmp row_count src where 1=1, src at hive2conn;
cmp row_count (select 'A' from src), src where 2=2 at hive2conn;

