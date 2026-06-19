DESCRIBE FUNCTION levenshtein;
DESC FUNCTION EXTENDED levenshtein;

explain select levenshtein('Test String1', 'Test String2');

select
levenshtein('kitten', 'sitting'),
levenshtein('Test String1', 'Test String2'),
levenshtein('Test String1', 'test String2'),
levenshtein('', 'Test String2'),
levenshtein('Test String1', ''),
levenshtein(cast(null as string), 'Test String2'),
levenshtein('Test String1', cast(null as string)),
levenshtein(cast(null as string), cast(null as string));