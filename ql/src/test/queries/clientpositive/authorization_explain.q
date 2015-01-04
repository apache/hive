set hive.security.authorization.enabled=true;

-- JAVA_VERSION_SPECIFIC_OUTPUT

explain authorization select * from src join srcpart;
explain formatted authorization select * from src join srcpart;

explain authorization use default;
explain formatted authorization use default;
