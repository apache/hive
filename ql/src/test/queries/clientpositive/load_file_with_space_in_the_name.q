-- test for loading into tables with the file with space in the name


CREATE TABLE load_file_with_space_in_the_name_n0(name STRING, age INT);
LOAD DATA LOCAL INPATH '../../data/files/person age.txt' INTO TABLE load_file_with_space_in_the_name_n0;
LOAD DATA LOCAL INPATH '../../data/files/person+age.txt' INTO TABLE load_file_with_space_in_the_name_n0;
