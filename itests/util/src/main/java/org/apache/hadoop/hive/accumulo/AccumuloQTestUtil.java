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
package org.apache.hadoop.hive.accumulo;

import org.apache.hadoop.hive.ql.QTestArguments;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.apache.hadoop.hive.ql.QTestUtil;

/**
 * AccumuloQTestUtil initializes Accumulo-specific test fixtures.
 */
public class AccumuloQTestUtil extends QTestUtil {

  public AccumuloQTestUtil(String outDir, String logDir, MiniClusterType miniMr,
      AccumuloTestSetup setup, String initScript, String cleanupScript) throws Exception {

    super(
        QTestArguments.QTestArgumentsBuilder.instance()
          .withOutDir(outDir)
          .withLogDir(logDir)
          .withClusterType(miniMr)
          .withConfDir(null)
          .withInitScript(initScript)
          .withCleanupScript(cleanupScript)
          .withLlapIo(false)
          .withQTestSetup(setup)
          .build());
  }
}
