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

import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.ReplCopyWork;
import org.apache.hadoop.hive.ql.parse.repl.CopyUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.parse.LoadSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.util.StringUtils;

public class ReplCopyTask extends Task<ReplCopyWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  private static transient final Logger LOG = LoggerFactory.getLogger(ReplCopyTask.class);

  public ReplCopyTask(){
    super();
  }

  @Override
  protected int execute(DriverContext driverContext) {
    LOG.debug("ReplCopyTask.execute()");
    FileSystem dstFs = null;
    Path toPath = null;
    try {
      // Note: CopyWork supports copying multiple files, but ReplCopyWork doesn't.
      //       Not clear of ReplCopyWork should inherit from CopyWork.
      if (work.getFromPaths().length > 1 || work.getToPaths().length > 1) {
        throw new RuntimeException("Invalid ReplCopyWork: "
            + work.getFromPaths() + ", " + work.getToPaths());
      }
      Path fromPath = work.getFromPaths()[0];
      toPath = work.getToPaths()[0];

      console.printInfo("Copying data from " + fromPath.toString(), " to "
          + toPath.toString());

      ReplCopyWork rwork = ((ReplCopyWork)work);

      FileSystem srcFs = fromPath.getFileSystem(conf);
      dstFs = toPath.getFileSystem(conf);

      // This should only be true for copy tasks created from functions, otherwise there should never
      // be a CM uri in the from path.
      if (ReplChangeManager.isCMFileUri(fromPath, srcFs)) {
        String[] result = ReplChangeManager.getFileWithChksumFromURI(fromPath.toString());
        ReplChangeManager.FileInfo sourceInfo = ReplChangeManager
            .getFileInfo(new Path(result[0]), result[1], conf);
        if (FileUtils.copy(
            sourceInfo.getSrcFs(), sourceInfo.getSourcePath(),
            dstFs, toPath, false, false, conf)) {
          return 0;
        } else {
          console.printError("Failed to copy: '" + fromPath.toString() + "to: '" + toPath.toString()
              + "'");
          return 1;
        }
      }

      List<ReplChangeManager.FileInfo> srcFiles = new ArrayList<>();
      if (rwork.readSrcAsFilesList()) {
        // This flow is usually taken for REPL LOAD
        // Our input is the result of a _files listing, we should expand out _files.
        srcFiles = filesInFileListing(srcFs, fromPath);
        if (LOG.isDebugEnabled()) {
          LOG.debug("ReplCopyTask _files contains: {}", (srcFiles == null ? "null" : srcFiles.size()));
        }
        if ((srcFiles == null) || (srcFiles.isEmpty())) {
          if (work.isErrorOnSrcEmpty()) {
            console.printError("No _files entry found on source: " + fromPath.toString());
            return 5;
          } else {
            return 0;
          }
        }
      } else {
        // This flow is usually taken for IMPORT command
        FileStatus[] srcs = LoadSemanticAnalyzer.matchFilesOrDir(srcFs, fromPath);
        if (LOG.isDebugEnabled()) {
          LOG.debug("ReplCopyTasks srcs= {}", (srcs == null ? "null" : srcs.length));
        }
        if (srcs == null || srcs.length == 0) {
          if (work.isErrorOnSrcEmpty()) {
            console.printError("No files matching path: " + fromPath.toString());
            return 3;
          } else {
            return 0;
          }
        }

        for (FileStatus oneSrc : srcs) {
          console.printInfo("Copying file: " + oneSrc.getPath().toString());
          LOG.debug("ReplCopyTask :cp:{}=>{}", oneSrc.getPath(), toPath);
          srcFiles.add(new ReplChangeManager.FileInfo(oneSrc.getPath().getFileSystem(conf),
                                                      oneSrc.getPath()));
        }
      }

      LOG.debug("ReplCopyTask numFiles: {}", srcFiles.size());
      if (!FileUtils.mkdir(dstFs, toPath, conf)) {
        console.printError("Cannot make target directory: " + toPath.toString());
        return 2;
      }
      // Copy the files from different source file systems to one destination directory
      new CopyUtils(rwork.distCpDoAsUser(), conf).copyAndVerify(dstFs, toPath, srcFiles);

      // If a file is copied from CM path, then need to rename them using original source file name
      // This is needed to avoid having duplicate files in target if same event is applied twice
      // where the first event refers to source path and  second event refers to CM path
      for (ReplChangeManager.FileInfo srcFile : srcFiles) {
        if (srcFile.isUseSourcePath()) {
          continue;
        }
        String destFileName = srcFile.getCmPath().getName();
        Path destFile = new Path(toPath, destFileName);
        if (dstFs.exists(destFile)) {
          String destFileWithSourceName = srcFile.getSourcePath().getName();
          Path newDestFile = new Path(toPath, destFileWithSourceName);
          boolean result = dstFs.rename(destFile, newDestFile);
          if (!result) {
            throw new IllegalStateException(
                "could not rename " + destFile.getName() + " to " + newDestFile.getName());
          }
        }
      }
      return 0;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      return (1);
    }
  }

  private List<ReplChangeManager.FileInfo> filesInFileListing(FileSystem fs, Path dataPath)
      throws IOException {
    Path fileListing = new Path(dataPath, EximUtil.FILES_NAME);
    LOG.debug("ReplCopyTask filesInFileListing() reading {}", fileListing.toUri());
    if (! fs.exists(fileListing)){
      LOG.debug("ReplCopyTask : _files does not exist");
      return null; // Returning null from this fn can serve as an err condition.
      // On success, but with nothing to return, we can return an empty list.
    }

    List<ReplChangeManager.FileInfo> filePaths = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileListing)))) {
      // TODO : verify if skipping charset here is okay

      String line = null;
      while ((line = br.readLine()) != null) {
        LOG.debug("ReplCopyTask :_filesReadLine: {}", line);

        String[] fileWithChksum = ReplChangeManager.getFileWithChksumFromURI(line);
        try {
          ReplChangeManager.FileInfo f = ReplChangeManager
              .getFileInfo(new Path(fileWithChksum[0]), fileWithChksum[1], conf);
          filePaths.add(f);
        } catch (MetaException e) {
          // issue warning for missing file and throw exception
          LOG.warn("Cannot find {} in source repo or cmroot", fileWithChksum[0]);
          throw new IOException(e.getMessage());
        }
        // Note - we need srcFs rather than fs, because it is possible that the _files lists files
        // which are from a different filesystem than the fs where the _files file itself was loaded
        // from. Currently, it is possible, for eg., to do REPL LOAD hdfs://<ip>/dir/ and for the _files
        // in it to contain hdfs://<name>/ entries, and/or vice-versa, and this causes errors.
        // It might also be possible that there will be a mix of them in a given _files file.
        // TODO: revisit close to the end of replv2 dev, to see if our assumption now still holds,
        // and if not so, optimize.
      }
    }
    return filePaths;
  }

  @Override
  public StageType getType() {
    return StageType.COPY;
    // there's no extensive need for this to have its own type - it mirrors
    // the intent of copy enough. This might change later, though.
  }

  @Override
  public String getName() {
    return "REPL_COPY";
  }

  public static Task<?> getLoadCopyTask(ReplicationSpec replicationSpec, Path srcPath, Path dstPath, HiveConf conf) {
    Task<?> copyTask = null;
    LOG.debug("ReplCopyTask:getLoadCopyTask: {}=>{}", srcPath, dstPath);
    if ((replicationSpec != null) && replicationSpec.isInReplicationScope()){
      ReplCopyWork rcwork = new ReplCopyWork(srcPath, dstPath, false);
      LOG.debug("ReplCopyTask:\trcwork");
      if (replicationSpec.isLazy()) {
        LOG.debug("ReplCopyTask:\tlazy");
        rcwork.setReadSrcAsFilesList(true);

        // It is assumed isLazy flag is set only for REPL LOAD flow.
        // IMPORT always do deep copy. So, distCpDoAsUser will be null by default in ReplCopyWork.
        String distCpDoAsUser = conf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
        rcwork.setDistCpDoAsUser(distCpDoAsUser);
      }
      copyTask = TaskFactory.get(rcwork, conf, true);
    } else {
      LOG.debug("ReplCopyTask:\tcwork");
      copyTask = TaskFactory.get(new CopyWork(srcPath, dstPath, false), conf, true);
    }
    return copyTask;
  }
}
