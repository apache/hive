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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * ServerUtils (specific to HiveServer version 1)
 */
public class ServerUtils {

  public static final Logger LOG = LoggerFactory.getLogger(ServerUtils.class);

  public static void cleanUpScratchDir(HiveConf hiveConf) {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_START_CLEANUP_SCRATCHDIR)) {
      String hiveScratchDir = hiveConf.get(HiveConf.ConfVars.SCRATCHDIR.varname);
      try {
        Path jobScratchDir = new Path(hiveScratchDir);
        LOG.info("Cleaning scratchDir : " + hiveScratchDir);
        FileSystem fileSystem = jobScratchDir.getFileSystem(hiveConf);
        fileSystem.delete(jobScratchDir, true);
      }
      // Even if the cleanup throws some exception it will continue.
      catch (Throwable e) {
        LOG.warn("Unable to delete scratchDir : " + hiveScratchDir, e);
      }
    }
  }

}
