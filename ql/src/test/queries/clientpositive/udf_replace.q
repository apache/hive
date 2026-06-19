DESCRIBE FUNCTION replace;
DESC FUNCTION EXTENDED replace;

select replace('', '', ''),
replace(null, '', ''),
replace('', null, ''),
replace('', '', null),
replace('Hack and Hue', 'H', 'BL'),
replace('ABABrdvABrk', 'AB', 'a');