package org.apache.hadoop.hive.cassandra;

import org.apache.hadoop.hive.ql.QTestUtil;

/**
 * CassandraQTestUtil initializes Cassandra-specific test fixtures.
 */
public class CassandraQTestUtil extends QTestUtil {
  public CassandraQTestUtil(
    String outDir, String logDir, boolean miniMr, CassandraTestSetup setup)
    throws Exception {

    super(outDir, logDir, miniMr, null);
    setup.preTest(conf, logDir);
    super.init();
  }

  @Override
  public void init() throws Exception {
    // defer
  }
}
