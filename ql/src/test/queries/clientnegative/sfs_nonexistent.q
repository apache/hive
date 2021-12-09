--! qt:replace:|file:/.*/nonexistent|FILE:///.../nonexistent|

create external table t1s (a string,b string,c string) location 'sfs+file://${system:test.tmp.dir}/nonexistent/path/f1.txt/#SINGLEFILE#';
