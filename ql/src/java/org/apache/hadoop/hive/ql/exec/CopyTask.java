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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.parse.LoadSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.util.StringUtils;

/**
 * CopyTask implementation.
 **/
public class CopyTask extends Task<CopyWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  private static transient final Log LOG = LogFactory.getLog(CopyTask.class);

  public CopyTask() {
    super();
  }

  @Override
  public int execute(DriverContext driverContext) {
    FileSystem dstFs = null;
    Path toPath = null;
    try {
      Path fromPath = work.getFromPath();
      toPath = work.getToPath();

      console.printInfo("Copying data from " + fromPath.toString(), " to "
          + toPath.toString());

      FileSystem srcFs = fromPath.getFileSystem(conf);
      dstFs = toPath.getFileSystem(conf);

      FileStatus[] srcs = LoadSemanticAnalyzer.matchFilesOrDir(srcFs, fromPath);
      if (srcs == null || srcs.length == 0) {
        if (work.isErrorOnSrcEmpty()) {
          console.printError("No files matching path: " + fromPath.toString());
          return 3;
        } else {
          return 0;
        }
      }

      boolean inheritPerms = conf.getBoolVar(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
      if (!FileUtils.mkdir(dstFs, toPath, inheritPerms, conf)) {
        console.printError("Cannot make target directory: " + toPath.toString());
        return 2;
      }

      for (FileStatus oneSrc : srcs) {
        console.printInfo("Copying file: " + oneSrc.getPath().toString());
        LOG.debug("Copying file: " + oneSrc.getPath().toString());
        if (!FileUtils.copy(srcFs, oneSrc.getPath(), dstFs, toPath,
            false, // delete source
            true, // overwrite destination
            conf)) {
          console.printError("Failed to copy: '" + oneSrc.getPath().toString()
              + "to: '" + toPath.toString() + "'");
          return 1;
        }
      }
      return 0;

    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      return (1);
    }
  }

  @Override
  public StageType getType() {
    return StageType.COPY;
  }

  @Override
  public String getName() {
    return "COPY";
  }
}
