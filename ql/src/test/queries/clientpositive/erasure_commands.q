--! qt:dataset:src
-- simple test to show ERASURE commands running on local fs (TestCliDriver) or hdfs (TestErasureCodingHDFSCliDriver).

ERASURE echo listOfPolicies output is:;
ERASURE listPolicies;

-- what is the policy on the root of the fs?
ERASURE echo original policy on /;
ERASURE getPolicy --path /;

