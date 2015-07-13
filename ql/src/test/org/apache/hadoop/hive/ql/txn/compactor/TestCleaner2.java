package org.apache.hadoop.hive.ql.txn.compactor;

/**
 * Same as TestCleaner but tests delta file names in Hive 1.3.0 format 
 */
public class TestCleaner2 extends TestCleaner {
  public TestCleaner2() throws Exception {
    super();
  }
  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }
}
