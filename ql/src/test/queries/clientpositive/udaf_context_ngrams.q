CREATE TABLE kafka_n0 (contents STRING);
LOAD DATA LOCAL INPATH '../../data/files/text-en.txt' INTO TABLE kafka_n0;
set mapred.reduce.tasks=1;
set hive.exec.reducers.max=1;

SELECT context_ngrams(sentences(lower(contents)), array(null), 100, 1000).estfrequency FROM kafka_n0;
SELECT context_ngrams(sentences(lower(contents)), array("he",null), 100, 1000) FROM kafka_n0;
SELECT context_ngrams(sentences(lower(contents)), array(null,"salesmen"), 100, 1000) FROM kafka_n0;
SELECT context_ngrams(sentences(lower(contents)), array("what","i",null), 100, 1000) FROM kafka_n0;
SELECT context_ngrams(sentences(lower(contents)), array(null,null), 100, 1000).estfrequency FROM kafka_n0;

DROP TABLE kafka_n0;
