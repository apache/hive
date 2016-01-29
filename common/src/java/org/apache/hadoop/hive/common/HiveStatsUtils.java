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
package org.apache.hadoop.hive.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HiveStatsUtils.
 * A collection of utilities used for hive statistics.
 * Used by classes in both metastore and ql package
 */

public class HiveStatsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveStatsUtils.class);

  /**
   * Get all file status from a root path and recursively go deep into certain levels.
   *
   * @param path
   *          the root path
   * @param level
   *          the depth of directory to explore
   * @param fs
   *          the file system
   * @return array of FileStatus
   * @throws IOException
   */
  public static FileStatus[] getFileStatusRecurse(Path path, int level, FileSystem fs)
      throws IOException {

    // if level is <0, the return all files/directories under the specified path
    if ( level < 0) {
      List<FileStatus> result = new ArrayList<FileStatus>();
      try {
        FileStatus fileStatus = fs.getFileStatus(path);
        FileUtils.listStatusRecursively(fs, fileStatus, result);
      } catch (IOException e) {
        // globStatus() API returns empty FileStatus[] when the specified path
        // does not exist. But getFileStatus() throw IOException. To mimic the
        // similar behavior we will return empty array on exception. For external
        // tables, the path of the table will not exists during table creation
        return new FileStatus[0];
      }
      return result.toArray(new FileStatus[result.size()]);
    }

    // construct a path pattern (e.g., /*/*) to find all dynamically generated paths
    StringBuilder sb = new StringBuilder(path.toUri().getPath());
    for (int i = 0; i < level; i++) {
      sb.append(Path.SEPARATOR).append("*");
    }
    Path pathPattern = new Path(path, sb.toString());
    return fs.globStatus(pathPattern, FileUtils.HIDDEN_FILES_PATH_FILTER);
  }

  public static int getNumBitVectorsForNDVEstimation(Configuration conf) throws Exception {
    int numBitVectors;
    float percentageError = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_STATS_NDV_ERROR);

    if (percentageError < 0.0) {
      throw new Exception("hive.stats.ndv.error can't be negative");
    } else if (percentageError <= 2.4) {
      numBitVectors = 1024;
      LOG.info("Lowest error achievable is 2.4% but error requested is " + percentageError + "%");
      LOG.info("Choosing 1024 bit vectors..");
    } else if (percentageError <= 3.4 ) {
      numBitVectors = 1024;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 1024 bit vectors..");
    } else if (percentageError <= 4.8) {
      numBitVectors = 512;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 512 bit vectors..");
     } else if (percentageError <= 6.8) {
      numBitVectors = 256;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 256 bit vectors..");
    } else if (percentageError <= 9.7) {
      numBitVectors = 128;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 128 bit vectors..");
    } else if (percentageError <= 13.8) {
      numBitVectors = 64;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 64 bit vectors..");
    } else if (percentageError <= 19.6) {
      numBitVectors = 32;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 32 bit vectors..");
    } else if (percentageError <= 28.2) {
      numBitVectors = 16;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 16 bit vectors..");
    } else if (percentageError <= 40.9) {
      numBitVectors = 8;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 8 bit vectors..");
    } else if (percentageError <= 61.0) {
      numBitVectors = 4;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 4 bit vectors..");
    } else {
      numBitVectors = 2;
      LOG.info("Error requested is " + percentageError + "%");
      LOG.info("Choosing 2 bit vectors..");
    }
    return numBitVectors;
  }

}
