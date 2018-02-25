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

import org.apache.hadoop.hive.ql.QTestUtil.MiniClusterType;

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
    public QTestProcessExecResult checkCliDriverResults(String tname) throws Exception {
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

    public CheckResults(String outDir, String logDir, MiniClusterType miniMr,
        String hadoopVer, String locationSubdir)
      throws Exception
    {
      super(outDir, logDir, miniMr, null, hadoopVer, "", "", false);
      this.locationSubdir = locationSubdir;
    }
  }

  /**
   * Verify that the location of the partition is valid. In this case
   * the path should end in "parta" and not "dt=a" (the default).
   *
   */
  public void testAlterTablePartitionLocation_alter5() throws Exception {
    String[] testNames = new String[] {"alter5.q"};

    File[] qfiles = setupQFiles(testNames);

    QTestUtil[] qt = new QTestUtil[qfiles.length];

    for (int i = 0; i < qfiles.length; i++) {
      qt[i] = new CheckResults(resDir, logDir, MiniClusterType.none, "0.20", "parta");
      qt[i].addFile(qfiles[i]);
      qt[i].clearTestSideEffects();
    }

    boolean success = QTestUtil.queryListRunnerSingleThreaded(qfiles, qt);
    if (!success) {
      fail("One or more queries failed");
    }
  }
}
