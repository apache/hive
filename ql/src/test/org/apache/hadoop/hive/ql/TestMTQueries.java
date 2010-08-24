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

package org.apache.hadoop.hive.ql;

import java.io.File;

import junit.framework.TestCase;

/**
 * Suite for testing running of queries in multi-threaded mode.
 */
public class TestMTQueries extends TestCase {

  private final String inpDir = System
      .getProperty("ql.test.query.clientpositive.dir");
  private final String resDir = System
      .getProperty("ql.test.results.clientpositive.dir");
  private final String logDir = System.getProperty("test.log.dir")
      + "/clientpositive";

  public void testMTQueries1() throws Exception {
    String[] testNames = new String[] {"join1.q", "join2.q", "groupby1.q",
        "groupby2.q", "join3.q", "input1.q", "input19.q"};
    String[] logDirs = new String[testNames.length];
    String[] resDirs = new String[testNames.length];
    File[] qfiles = new File[testNames.length];
    for (int i = 0; i < resDirs.length; i++) {
      logDirs[i] = logDir;
      resDirs[i] = resDir;
      qfiles[i] = new File(inpDir, testNames[i]);
    }

    boolean success = QTestUtil.queryListRunner(qfiles, resDirs, logDirs, true, this);
    if (!success) {
      fail("One or more queries failed");
    }
  }
}
