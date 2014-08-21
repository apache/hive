/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.QTestUtil;

import java.util.List;

/**
 * HBaseQTestUtil initializes HBase-specific test fixtures.
 */
public class HBaseQTestUtil extends QTestUtil {

  /** Name of the HBase table, in both Hive and HBase. */
  public static String HBASE_SRC_NAME = "src_hbase";

  /** Name of the table snapshot. */
  public static String HBASE_SRC_SNAPSHOT_NAME = "src_hbase_snapshot";

  /** A handle to this harness's cluster */
  private final HConnection conn;

  public HBaseQTestUtil(
    String outDir, String logDir, MiniClusterType miniMr, HBaseTestSetup setup,
    String initScript, String cleanupScript)
    throws Exception {

    super(outDir, logDir, miniMr, null, initScript, cleanupScript);
    setup.preTest(conf);
    this.conn = setup.getConnection();
    super.init();
  }

  /** return true when HBase table snapshot exists, false otherwise. */
  private static boolean hbaseTableSnapshotExists(HBaseAdmin admin, String snapshotName) throws
      Exception {
    List<HBaseProtos.SnapshotDescription> snapshots =
      admin.listSnapshots(".*" + snapshotName + ".*");
    for (HBaseProtos.SnapshotDescription sn : snapshots) {
      if (sn.getName().equals(HBASE_SRC_SNAPSHOT_NAME)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void init() throws Exception {
    // defer
  }

  @Override
  public void createSources() throws Exception {
    super.createSources();

    conf.setBoolean("hive.test.init.phase", true);

    // create and load the input data into the hbase table
    runCreateTableCmd(
      "CREATE TABLE " + HBASE_SRC_NAME + "(key INT, value STRING)"
        + "  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
        + "  WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,cf:val')"
        + "  TBLPROPERTIES ('hbase.table.name' = '" + HBASE_SRC_NAME + "')"
    );
    runCmd("INSERT OVERWRITE TABLE " + HBASE_SRC_NAME + " SELECT * FROM src");

    // create a snapshot
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(conn.getConfiguration());
      admin.snapshot(HBASE_SRC_SNAPSHOT_NAME, HBASE_SRC_NAME);
    } finally {
      if (admin != null) admin.close();
    }

    conf.setBoolean("hive.test.init.phase", false);
  }

  @Override
  public void cleanUp() throws Exception {
    super.cleanUp();

    // drop in case leftover from unsuccessful run
    db.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, HBASE_SRC_NAME);

    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(conn.getConfiguration());
      if (hbaseTableSnapshotExists(admin, HBASE_SRC_SNAPSHOT_NAME)) {
        admin.deleteSnapshot(HBASE_SRC_SNAPSHOT_NAME);
      }
    } finally {
      if (admin != null) admin.close();
    }
  }
}
