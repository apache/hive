-- Debug instructions:
-- 1. Run the test in debug mode: mvn test -Dtest=TestMiniLlapLocalCliDriver -Dqfile=partition_descriptor_reuse.q -Dtest.output.overwrite -Dtest.metastore.db=postgres -Dmaven.surefire.debug
-- 2. Put a breakpoint in CliAdapter.java:120 CliAdapter.this.tearDown() to prevent DB from being dropped.
-- 3. Connect to the dockerized database and check the state of the metadata
-- docker exec -it CONTAINER_NAME psql -U test -d hivedb
-- TODO: Probably not gonna need this file at the end so drop it
create table person(id string, fname string) partitioned by (birthyear string);
alter table person add partition (birthyear='1987');
alter table person add partition (birthyear='1988');
alter table person add partition (birthyear='1989');
alter table person add partition (birthyear='1990');
alter table person add partition (birthyear='1991');

alter table person partition (birthyear='1989') add columns (lname string);
alter table person partition (birthyear='1990') add columns (lname string);
alter table person partition (birthyear='1991') change column fname fullname string;
