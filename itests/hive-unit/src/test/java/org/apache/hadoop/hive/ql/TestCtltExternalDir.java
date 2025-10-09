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

import java.io.File;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.fail;

public class TestCtltExternalDir extends BaseTestQueries {
  public TestCtltExternalDir() {
    File logDirFile = new File(logDir);
    if (!(logDirFile.exists() || logDirFile.mkdirs())) {
      fail("Could not create " + logDir);
    }
  }

  @Test public void testCtltLocation() throws Exception {
    String[] testNames = new String[] { "ctlt_translate_external.q" };

    File[] qfiles = setupQFiles(testNames);

    String whRootExternal = "/tmp/wh_ext";

    QTestUtil qt = new QTestUtil(
        QTestArguments.QTestArgumentsBuilder.instance().withOutDir(resDir + "/llap").withLogDir(logDir).withClusterType(MiniClusterType.LLAP_LOCAL).withConfDir(null).withInitScript("").withCleanupScript("")
            .withLlapIo(false).build());

    HiveConf hiveConf = qt.getConf();
    hiveConf.setVar(ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL, whRootExternal);
    TestTxnDbUtil.setConfValues(hiveConf);
    TestTxnDbUtil.cleanDb(hiveConf);
    TestTxnDbUtil.prepDb(hiveConf);
    qt.postInit();
    qt.newSession();
    qt.setInputFile(qfiles[0]);
    qt.clearTestSideEffects();

    boolean success = QTestRunnerUtils.queryListRunnerSingleThreaded(qfiles, new QTestUtil[] { qt });
    if (success) {
      IMetaStoreClient hmsClient = new HiveMetaStoreClient(hiveConf);
      Table table = hmsClient.getTable("default", "test_ext1");
      FileSystem fs = FileSystem.get(hiveConf);
      String location = table.getSd().getLocation();
      Assert.assertEquals("Not an external table", "file:" + whRootExternal + "/test_ext1", location);
    } else {
      fail("One or more queries failed");
    }
  }
}