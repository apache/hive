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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FileOperations;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.util.StringUtils;

/**
 * CopyTask implementation.
 **/
public class CopyTask extends Task<CopyWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private static transient final Logger LOG = LoggerFactory.getLogger(CopyTask.class);

  public CopyTask() {
    super();
  }

  @Override
  public int execute(DriverContext driverContext) {
    Path[] from = work.getFromPaths(), to = work.getToPaths();
    for (int i = 0; i < from.length; ++i) {
      int result = copyOnePath(from[i], to[i]);
      if (result != 0) return result;
    }
    return 0;
  }

  protected int copyOnePath(Path fromPath, Path toPath) {
    FileSystem dstFs = null;
    try {
      Utilities.FILE_OP_LOGGER./**/debug("Copying data from {} to {} " + fromPath);
      console.printInfo("Copying data from " + fromPath.toString(), " to "
          + toPath.toString());

      FileSystem srcFs = fromPath.getFileSystem(conf);
      dstFs = toPath.getFileSystem(conf);

      FileStatus[] srcs = matchFilesOrDir(srcFs, fromPath, work.doSkipSourceMmDirs());
      if (srcs == null || srcs.length == 0) {
        if (work.isErrorOnSrcEmpty()) {
          console.printError("No files matching path: " + fromPath.toString());
          return 3;
        } else {
          return 0;
        }
      }

      if (!FileUtils.mkdir(dstFs, toPath, conf)) {
        console.printError("Cannot make target directory: " + toPath.toString());
        return 2;
      }

      for (FileStatus oneSrc : srcs) {
        String oneSrcPathStr = oneSrc.getPath().toString();
        console.printInfo("Copying file: " + oneSrcPathStr);
        Utilities.FILE_OP_LOGGER.debug("Copying file {} to {}", oneSrcPathStr, toPath);
        if (!FileUtils.copy(srcFs, oneSrc.getPath(), dstFs, toPath,
            false, // delete source
            true, // overwrite destination
            conf)) {
          console.printError("Failed to copy: '" + oneSrcPathStr
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

  // Note: initially copied from LoadSemanticAnalyzer.
  private static FileStatus[] matchFilesOrDir(
      FileSystem fs, Path path, boolean isSourceMm) throws IOException {
    if (!fs.exists(path)) return null;
    if (!isSourceMm) return matchFilesOneDir(fs, path, null);
    // Note: this doesn't handle list bucketing properly; neither does the original code.
    FileStatus[] mmDirs = fs.listStatus(path, new JavaUtils.AnyIdDirFilter());
    if (mmDirs == null || mmDirs.length == 0) return null;
    List<FileStatus> allFiles = new ArrayList<FileStatus>();
    for (FileStatus mmDir : mmDirs) {
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("Found source MM directory " + mmDir.getPath());
      }
      matchFilesOneDir(fs, mmDir.getPath(), allFiles);
    }
    return allFiles.toArray(new FileStatus[allFiles.size()]);
  }

  private static FileStatus[] matchFilesOneDir(
      FileSystem fs, Path path, List<FileStatus> result) throws IOException {
    FileStatus[] srcs = fs.globStatus(path, new EximPathFilter());
    if (srcs != null && srcs.length == 1) {
      if (srcs[0].isDirectory()) {
        srcs = fs.listStatus(srcs[0].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
      }
    }
    if (result != null && srcs != null) {
      for (int i = 0; i < srcs.length; ++i) {
        result.add(srcs[i]);
      }
    }
    return srcs;
  }

  private static final class EximPathFilter implements PathFilter {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return name.equals("_metadata") ? true : !name.startsWith("_") && !name.startsWith(".");
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
