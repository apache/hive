set hive.vectorized.execution.enabled=true;

-- HIVE-18191

create table cd
(
    cd_demo_sk                int,
    cd_gender                 string,
    cd_marital_status         string,
    cd_purchase_estimate      int,
    cd_credit_rating          string,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int
)
partitioned by
(
    cd_education_status       string
);
alter table cd add partition (cd_education_status='Primary');
insert into table cd partition (cd_education_status='Primary') values (1, 'M', 'M', 500, 'Good', 0, 0, 0);

explain vectorization detail
analyze table cd partition (cd_education_status) compute statistics;

analyze table cd partition (cd_education_status) compute statistics;