A = load '$INPDIR/table3.txt' using PigStorage('\t') AS (row:int, content:chararray);
B = load '$INPDIR/table3ToJoin.txt' using PigStorage('\t') AS (row:int, content:chararray);
C = JOIN A BY content, B BY content;
store C into '$OUTDIR/PigJoin' USING PigStorage();