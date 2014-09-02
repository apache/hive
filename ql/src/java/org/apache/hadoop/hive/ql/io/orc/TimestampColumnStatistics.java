package org.apache.hadoop.hive.ql.io.orc;

import java.sql.Timestamp;

/**
 * Statistics for Timestamp columns.
 */
public interface TimestampColumnStatistics extends ColumnStatistics {
  /**
   * Get the minimum value for the column.
   * @return minimum value
   */
  Timestamp getMinimum();

  /**
   * Get the maximum value for the column.
   * @return maximum value
   */
  Timestamp getMaximum();
}
