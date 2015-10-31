CREATE TABLE skewed_table (key STRING, value STRING) SKEWED BY (key_non) ON ((1),(5),(6));
