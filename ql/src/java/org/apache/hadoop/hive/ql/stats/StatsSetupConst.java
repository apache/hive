package org.apache.hadoop.hive.ql.stats;

/**
 * A class that defines the constant strings used by the statistics implementation.
 */

public class StatsSetupConst {

  /**
   * The value of the user variable "hive.stats.dbclass" to use HBase implementation.
   */
  public static final String HBASE_IMPL_CLASS_VAL = "hbase";

  /**
   * The value of the user variable "hive.stats.dbclass" to use JDBC implementation.
   */
  public static final String JDBC_IMPL_CLASS_VAL = "jdbc";

  /**
   * The name of the statistic Row Count to be published or gathered.
   */
  public static final String ROW_COUNT = "numRows";

  /**
   * The name of the statistic Row Count to be published or gathered.
   */
  public static final String NUM_FILES = "numFiles";

  /**
   * The name of the statistic Row Count to be published or gathered.
   */
  public static final String NUM_PARTITIONS = "numPartitions";

  /**
   * The name of the statistic Row Count to be published or gathered.
   */
  public static final String TOTAL_SIZE = "totalSize";

}
