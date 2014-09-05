package org.apache.hadoop.hive.ql.optimizer.optiq;

import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveRelMdDistinctRowCount;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveRelMdRowCount;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveRelMdSelectivity;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveRelMdUniqueKeys;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.metadata.DefaultRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataProvider;

/**
 * Distinct row count and Selectivity is overridden for Hive.<br>
 * <p>
 * Distinct Row Count is overridden for:<br>
 * 1) Join 2) TableScan.<br>
 * Selectivity is overridden for:<br>
 * 1) Join 2) TableScan & Filter.
 */
public class HiveDefaultRelMetadataProvider {
  private HiveDefaultRelMetadataProvider() {
  }

  public static final RelMetadataProvider INSTANCE = ChainedRelMetadataProvider.of(ImmutableList
                                                       .of(HiveRelMdDistinctRowCount.SOURCE,
                                                           HiveRelMdSelectivity.SOURCE,
                                                           HiveRelMdRowCount.SOURCE,
                                                           HiveRelMdUniqueKeys.SOURCE,
                                                           new DefaultRelMetadataProvider()));
}
