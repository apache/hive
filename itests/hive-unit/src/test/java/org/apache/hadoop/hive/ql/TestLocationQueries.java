/*
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

package org.apache.hadoop.hive.ql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.fail;

/**
 * Suite for testing location. e.g. if "alter table alter partition
 * location" is run, do the partitions end up in the correct location.
 *
 *  This is a special case of the regular queries as paths are typically
 *  ignored.
 */
public class TestLocationQueries extends BaseTestQueries {

  public TestLocationQueries() {
    File logDirFile = new File(logDir);
    if (!(logDirFile.exists() || logDirFile.mkdirs())) {
      fail("Could not create " + logDir);
    }
  }

  /**
   * Our own checker - validate the location of the partition.
   */
  public static class CheckResults extends QTestUtil {
    private final String locationSubdir;

    /**
     * Validate only that the location is correct.
     * @return non-zero if it failed
     */
    @Override
    public QTestProcessExecResult checkCliDriverResults() throws Exception {
      String tname = getInputFile().getName();
      File logFile = new File(logDir, tname + ".out");

      int failedCount = 0;
      StringBuilder fileNames = new StringBuilder("Files failing the location check:");
      FileReader fr = new FileReader(logFile);
      BufferedReader in = new BufferedReader(fr);
      try {
        String line;
        int locationCount = 0;
        Pattern p = Pattern.compile("location:([^,)]+)");
        while((line = in.readLine()) != null) {
          Matcher m = p.matcher(line);
          if (m.find()) {
            File f = new File(m.group(1));
            if (!f.getName().equals(locationSubdir)) {
              failedCount++;
              fileNames.append(f.getName()).append("\r\n");
            }
            locationCount++;
          }
        }
        // we always have to find at least one location, otw the test is useless
        if (locationCount == 0) {
          return QTestProcessExecResult.create(Integer.MAX_VALUE, "0 locations tested");
        }
      } finally {
        in.close();
      }

      return QTestProcessExecResult.create(failedCount, fileNames.toString());
    }

    public CheckResults(String outDir, String logDir, MiniClusterType miniMr, String locationSubdir)
      throws Exception
    {
      super(
          QTestArguments.QTestArgumentsBuilder.instance()
            .withOutDir(outDir)
            .withLogDir(logDir)
            .withClusterType(miniMr)
            .withConfDir(null)
            .withInitScript("")
            .withCleanupScript("")
            .withLlapIo(false)
            .build());

      this.locationSubdir = locationSubdir;
    }
  }

  /**
   * Verify that the location of the partition is valid. In this case
   * the path should end in "parta" and not "dt=a" (the default).
   *
   */
  @Test
  public void testAlterTablePartitionLocation_alter5() throws Exception {
    String[] testNames = new String[] {"alter5.q"};

    File[] qfiles = setupQFiles(testNames);

    QTestUtil[] qt = new QTestUtil[qfiles.length];

    for (int i = 0; i < qfiles.length; i++) {
      qt[i] = new CheckResults(resDir, logDir, MiniClusterType.NONE, "parta");
      qt[i].postInit();
      qt[i].newSession();
      qt[i].setInputFile(qfiles[i]);
      qt[i].clearTestSideEffects();
    }

    boolean success = QTestRunnerUtils.queryListRunnerSingleThreaded(qfiles, qt);
    if (!success) {
      fail("One or more queries failed");
    }
  }

  /**
   * Verify the delta directory name of the load data inpath command for MM acid tables.
   */
  @Test
  public void testAcidLoadDataLocation() throws Exception {
    String[] testNames = new String[]{"acid_load_data.q"};

    File[] qfiles = setupQFiles(testNames);

    QTestUtil qt = new QTestUtil(QTestArguments.QTestArgumentsBuilder.instance()
            .withOutDir(resDir + "/llap")
            .withLogDir(logDir)
            .withClusterType(MiniClusterType.LLAP_LOCAL)
            .withConfDir(null)
            .withInitScript("")
            .withCleanupScript("")
            .withLlapIo(false)
            .build());

    HiveConf hiveConf = qt.getConf();
    TestTxnDbUtil.setConfValues(hiveConf);
    TestTxnDbUtil.cleanDb(hiveConf);
    TestTxnDbUtil.prepDb(hiveConf);
    qt.postInit();
    qt.newSession();
    qt.setInputFile(qfiles[0]);
    qt.clearTestSideEffects();

    boolean success = QTestRunnerUtils.queryListRunnerSingleThreaded(qfiles, new QTestUtil[]{qt});
    if (success) {
      IMetaStoreClient hmsClient = new HiveMetaStoreClient(hiveConf);
      Table table = hmsClient.getTable("default", "kv_mm");
      FileSystem fs = FileSystem.get(hiveConf);
      String location = table.getSd().getLocation();
      Path delta = fs.listStatus(new Path(location))[0].getPath();
      Assert.assertEquals("Delta directory name mismatch!", "delta_0000001_0000001_0000", delta.getName());
    } else {
      fail("One or more queries failed");
    }
  }
}
