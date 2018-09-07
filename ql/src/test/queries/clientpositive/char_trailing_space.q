create table char_trailing_space(a char(2), b varchar(2));
insert into char_trailing_space values('L ', 'L ');

select length(a),length(b) from char_trailing_space;
select character_length(a),character_length(b) from char_trailing_space;
select octet_length(a),octet_length(b) from char_trailing_space;

drop table char_trailing_space;
