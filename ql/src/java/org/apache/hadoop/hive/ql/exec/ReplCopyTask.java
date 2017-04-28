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

import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.ReplCopyWork;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
      Path fromPath = work.getFromPath();
      toPath = work.getToPath();

      console.printInfo("Copying data from " + fromPath.toString(), " to "
          + toPath.toString());

      ReplCopyWork rwork = ((ReplCopyWork)work);

      FileSystem srcFs = fromPath.getFileSystem(conf);
      dstFs = toPath.getFileSystem(conf);

      List<FileStatus> srcFiles = new ArrayList<FileStatus>();
      FileStatus[] srcs = LoadSemanticAnalyzer.matchFilesOrDir(srcFs, fromPath);
      LOG.debug("ReplCopyTasks srcs=" + (srcs == null ? "null" : srcs.length));
      if (! rwork.getReadListFromInput()){
        if (srcs == null || srcs.length == 0) {
          if (work.isErrorOnSrcEmpty()) {
            console.printError("No files matching path: " + fromPath.toString());
            return 3;
          } else {
            return 0;
          }
        }
      } else {
        LOG.debug("ReplCopyTask making sense of _files");
        // Our input is probably the result of a _files listing, we should expand out _files.
        srcFiles = filesInFileListing(srcFs,fromPath);
        LOG.debug("ReplCopyTask _files contains:" + (srcFiles == null ? "null" : srcFiles.size()));
        if (srcFiles == null){
          if (work.isErrorOnSrcEmpty()) {
            console.printError("No _files entry found on source: " + fromPath.toString());
            return 5;
          } else {
            return 0;
          }
        }
      }
      // Add in all the lone filecopies expected as well - applies to
      // both _files case stragglers and regular copies
      srcFiles.addAll(Arrays.asList(srcs));
      LOG.debug("ReplCopyTask numFiles:" + (srcFiles == null ? "null" : srcFiles.size()));

      boolean inheritPerms = conf.getBoolVar(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
      if (!FileUtils.mkdir(dstFs, toPath, inheritPerms, conf)) {
        console.printError("Cannot make target directory: " + toPath.toString());
        return 2;
      }

      BufferedWriter listBW = null;
      if (rwork.getListFilesOnOutputBehaviour()){
        Path listPath = new Path(toPath,EximUtil.FILES_NAME);
        LOG.debug("ReplCopyTask : generating _files at :" + listPath.toUri().toString());
        if (dstFs.exists(listPath)){
          console.printError("Cannot make target _files file:" + listPath.toString());
          return 4;
        }
        listBW = new BufferedWriter(new OutputStreamWriter(dstFs.create(listPath)));
        // TODO : verify that not specifying charset here does not bite us
        // later(for cases where filenames have unicode chars)
      }

      for (FileStatus oneSrc : srcFiles) {
        console.printInfo("Copying file: " + oneSrc.getPath().toString());
        LOG.debug("Copying file: " + oneSrc.getPath().toString());

        FileSystem actualSrcFs = null;
        if (rwork.getReadListFromInput()){
          // TODO : filesystemcache prevents this from being a perf nightmare, but we
          // should still probably follow up to see if we need to do something better here.
          actualSrcFs = oneSrc.getPath().getFileSystem(conf);
        } else {
          actualSrcFs = srcFs;
        }
        if (!rwork.getListFilesOnOutputBehaviour(oneSrc)){

          LOG.debug("ReplCopyTask :cp:" + oneSrc.getPath() + "=>" + toPath);
          if (!FileUtils.copy(actualSrcFs, oneSrc.getPath(), dstFs, toPath,
            false, // delete source
            true, // overwrite destination
            conf)) {
          console.printError("Failed to copy: '" + oneSrc.getPath().toString()
              + "to: '" + toPath.toString() + "'");
          return 1;
          }
        }else{
          LOG.debug("ReplCopyTask _files now tracks:" + oneSrc.getPath().toUri());
          console.printInfo("Tracking file: " + oneSrc.getPath().toUri());
          String chksumString = ReplChangeManager.getChksumString(oneSrc.getPath(), actualSrcFs);
          listBW.write(ReplChangeManager.encodeFileUri
              (oneSrc.getPath().toUri().toString(), chksumString) + "\n");
        }
      }

      if (listBW != null){
        listBW.close();
      }

      return 0;

    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      return (1);
    }
  }


  private List<FileStatus> filesInFileListing(FileSystem fs, Path path)
      throws IOException {
    Path fileListing = new Path(path, EximUtil.FILES_NAME);
    LOG.debug("ReplCopyTask filesInFileListing() reading " + fileListing.toUri());
    if (! fs.exists(fileListing)){
      LOG.debug("ReplCopyTask : _files does not exist");
      return null; // Returning null from this fn can serve as an err condition.
      // On success, but with nothing to return, we can return an empty list.
    }

    List<FileStatus> ret = new ArrayList<FileStatus>();
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileListing)));
    // TODO : verify if skipping charset here is okay

    String line = null;
    while ( (line = br.readLine()) != null){
      LOG.debug("ReplCopyTask :_filesReadLine:" + line);

      String[] fileWithChksum = ReplChangeManager.getFileWithChksumFromURI(line);
      try {
        FileStatus f = ReplChangeManager.getFileStatus(new Path(fileWithChksum[0]),
            fileWithChksum[1], conf);
        ret.add(f);
      } catch (MetaException e) {
        // skip and issue warning for missing file
        LOG.warn("Cannot find " + fileWithChksum[0] + " in source repo or cmroot");
      }
      // Note - we need srcFs rather than fs, because it is possible that the _files lists files
      // which are from a different filesystem than the fs where the _files file itself was loaded
      // from. Currently, it is possible, for eg., to do REPL LOAD hdfs://<ip>/dir/ and for the _files
      // in it to contain hdfs://<name>/ entries, and/or vice-versa, and this causes errors.
      // It might also be possible that there will be a mix of them in a given _files file.
      // TODO: revisit close to the end of replv2 dev, to see if our assumption now still holds,
      // and if not so, optimize.
    }

    return ret;
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
    LOG.debug("ReplCopyTask:getLoadCopyTask: "+srcPath + "=>" + dstPath);
    if (replicationSpec.isInReplicationScope()){
      ReplCopyWork rcwork = new ReplCopyWork(srcPath, dstPath, false);
      LOG.debug("ReplCopyTask:\trcwork");
      if (replicationSpec.isLazy()){
        LOG.debug("ReplCopyTask:\tlazy");
        rcwork.setReadListFromInput(true);
      }
      copyTask = TaskFactory.get(rcwork, conf);
    } else {
      LOG.debug("ReplCopyTask:\tcwork");
      copyTask = TaskFactory.get(new CopyWork(srcPath, dstPath, false), conf);
    }
    return copyTask;
  }

  public static Task<?> getDumpCopyTask(ReplicationSpec replicationSpec, Path srcPath, Path dstPath, HiveConf conf) {
    Task<?> copyTask = null;
    LOG.debug("ReplCopyTask:getDumpCopyTask: "+srcPath + "=>" + dstPath);
    if (replicationSpec.isInReplicationScope()){
      ReplCopyWork rcwork = new ReplCopyWork(srcPath, dstPath, false);
      LOG.debug("ReplCopyTask:\trcwork");
      if (replicationSpec.isLazy()){
        LOG.debug("ReplCopyTask:\tlazy");
        rcwork.setListFilesOnOutputBehaviour(true);
      }
      copyTask = TaskFactory.get(rcwork, conf);
    } else {
      LOG.debug("ReplCopyTask:\tcwork");
      copyTask = TaskFactory.get(new CopyWork(srcPath, dstPath, false), conf);
    }
    return copyTask;
  }

}
