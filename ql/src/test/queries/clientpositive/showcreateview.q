CREATE VIEW ViewWithSpecialChars AS SELECT key FROM src where key = 'abcÖdefÖgh';
SHOW CREATE TABLE ViewWithSpecialChars;
