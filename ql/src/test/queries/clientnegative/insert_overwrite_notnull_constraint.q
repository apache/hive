create table nullConstraintCheck(i int NOT NULL enforced, j int);
insert overwrite table nullConstraintCheck values(null,2);

