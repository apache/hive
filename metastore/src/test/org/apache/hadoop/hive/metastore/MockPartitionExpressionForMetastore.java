package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.List;

/**
 * Test Mock-out for PartitionExpressionForMetastore.
 */
public class MockPartitionExpressionForMetastore implements PartitionExpressionProxy {
  @Override
  public String convertExprToFilter(byte[] expr) throws MetaException {
    return null;
  }

  @Override
  public boolean filterPartitionsByExpr(List<String> columnNames, byte[] expr, String defaultPartitionName, List<String> partitionNames) throws MetaException {
    return false;
  }
}
