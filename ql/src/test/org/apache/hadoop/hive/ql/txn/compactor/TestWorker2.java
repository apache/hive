package org.apache.hadoop.hive.ql.txn.compactor;

/**
 * Same as TestWorker but tests delta file names in Hive 1.3.0 format 
 */
public class TestWorker2 extends TestWorker {

  public TestWorker2() throws Exception {
    super();
  }

  @Override
  boolean useHive130DeltaDirName() {
    return true;
  }
}
