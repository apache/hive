package org.apache.hadoop.hive.ql.cube.parse;

public interface CubeQueryConstants {
  public static final String VALID_FACT_TABLES = "cube.query.valid.fact.tables";
  public static final String VALID_STORAGE_FACT_TABLES = "cube.query.valid." +
      "fact.storagetables";
  public static final String VALID_STORAGE_DIM_TABLES = "cube.query.valid." +
      "dim.storgaetables";
  public static final String DRIVER_SUPPORTED_STORAGES = "cube.query.driver." +
      "supported.storages";
  public static final String FAIL_QUERY_ON_PARTIAL_DATA =
      "cube.query.fail.if.data.partial";
  public static final String NON_EXISTING_PARTITIONS =
      "cube.query.nonexisting.partitions";
}
