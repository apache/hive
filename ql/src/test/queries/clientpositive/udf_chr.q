DESCRIBE FUNCTION chr;
DESC FUNCTION EXTENDED chr;

select chr(-1),
chr(0Y),
chr(1Y),
chr(48Y),
chr(65Y),

chr(0S),
chr(1S),
chr(48S),
chr(65S),
chr(321S),

chr(0L),
chr(1L),
chr(48L),
chr(65L),
chr(321L),

chr(cast(68.12 as float)),
chr(cast(68.12 as double)),
chr(cast(321.12 as double)),
chr(32457964L);