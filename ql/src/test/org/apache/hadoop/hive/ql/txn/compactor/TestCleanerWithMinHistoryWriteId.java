package org.apache.hadoop.hive.ql.txn.compactor;

public class TestCleanerWithMinHistoryWriteId extends TestCleaner {
  @Override
  protected boolean useMinHistoryWriteId() {
    return true;
  }
}
