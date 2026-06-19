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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.ql.Context.generateExecutionId;

public class PathInfo {
  private static Logger LOG = LoggerFactory.getLogger(PathUtils.class);

  private final Map<String, Path> fsScratchDirs = new HashMap<>();
  private final String stagingDir;
  private final HiveConf hiveConf;

  public PathInfo(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    stagingDir = HiveConf.getVar(hiveConf, HiveConf.ConfVars.STAGING_DIR);
  }

  public Map<String, Path> getFsScratchDirs() {
    return fsScratchDirs;
  }

  Path computeStagingDir(Path inputPath) {
    final URI inputPathUri = inputPath.toUri();
    final String inputPathName = inputPathUri.getPath();
    final String fileSystemAsString = inputPathUri.getScheme() + ":" + inputPathUri.getAuthority();

    String stagingPathName;
    if (!inputPathName.contains(stagingDir)) {
      stagingPathName = new Path(inputPathName, stagingDir).toString();
    } else {
      stagingPathName =
          inputPathName.substring(0, inputPathName.indexOf(stagingDir) + stagingDir.length());
    }

    final String key =
        fileSystemAsString + "-" + stagingPathName + "-" + TaskRunner.getTaskRunnerID();

    Path dir = fsScratchDirs.get(key);
    try {
      FileSystem fileSystem = inputPath.getFileSystem(hiveConf);
      if (dir == null) {
        // Append task specific info to stagingPathName, instead of creating a sub-directory.
        // This way we don't have to worry about deleting the stagingPathName separately at
        // end of query execution.
        Path path = new Path(
            stagingPathName + "_" + generateExecutionId() + "-" + TaskRunner.getTaskRunnerID());
        dir = fileSystem.makeQualified(path);

        LOG.debug("Created staging dir = " + dir + " for path = " + inputPath);

        if (!FileUtils.mkdir(fileSystem, dir, hiveConf)) {
          throw new IllegalStateException(
              "Cannot create staging directory  '" + dir.toString() + "'");
        }
        fileSystem.deleteOnExit(dir);
      }
      fsScratchDirs.put(key, dir);
      return dir;

    } catch (IOException e) {
      throw new RuntimeException(
          "Cannot create staging directory '" + dir.toString() + "': " + e.getMessage(), e);
    }
  }
}
