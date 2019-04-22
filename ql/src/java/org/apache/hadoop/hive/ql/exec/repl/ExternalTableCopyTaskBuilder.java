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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.parse.repl.CopyUtils;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import java.security.PrivilegedExceptionAction;

import java.io.Serializable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ExternalTableCopyTaskBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalTableCopyTaskBuilder.class);
  private final ReplLoadWork work;
  private final HiveConf conf;

  ExternalTableCopyTaskBuilder(ReplLoadWork work, HiveConf conf) {
    this.work = work;
    this.conf = conf;
  }

  List<Task<? extends Serializable>> tasks(TaskTracker tracker) {
    List<Task<? extends Serializable>> tasks = new ArrayList<>();
    Iterator<DirCopyWork> itr = work.getPathsToCopyIterator();
    while (tracker.canAddMoreTasks() && itr.hasNext()) {
      DirCopyWork dirCopyWork = itr.next();
      Task<DirCopyWork> task = TaskFactory.get(dirCopyWork, conf);
      tasks.add(task);
      tracker.addTask(task);
      LOG.debug("added task for {}", dirCopyWork);
    }
    return tasks;
  }

  public static class DirCopyTask extends Task<DirCopyWork> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DirCopyTask.class);
    private static final int MAX_COPY_RETRY = 5;

    private boolean createAndSetPathOwner(Path destPath, Path sourcePath) throws IOException {
      FileSystem targetFs = destPath.getFileSystem(conf);
      boolean createdDir = false;
      if (!targetFs.exists(destPath)) {
        // target path is created even if the source path is missing, so that ddl task does not try to create it.
        if (!targetFs.mkdirs(destPath)) {
          throw new IOException(destPath + " is not a directory or unable to create one");
        }
        createdDir = true;
      }

      FileStatus status;
      try {
        status = sourcePath.getFileSystem(conf).getFileStatus(sourcePath);
      } catch (FileNotFoundException e) {
        // Don't delete target path created else ddl task will try to create it using user hive and may fail.
        LOG.warn("source path missing " + sourcePath);
        return createdDir;
      }
      LOG.info("Setting permission for path dest {} from source {} owner {} : {} : {}",
              destPath, sourcePath, status.getOwner(), status.getGroup(), status.getPermission());
      destPath.getFileSystem(conf).setOwner(destPath, status.getOwner(), status.getGroup());
      destPath.getFileSystem(conf).setPermission(destPath, status.getPermission());
      return createdDir;
    }

    private boolean setTargetPathOwner(Path targetPath, Path sourcePath, UserGroupInformation proxyUser)
            throws IOException, InterruptedException {
      if (proxyUser == null) {
        return createAndSetPathOwner(targetPath, sourcePath);
      }
      return proxyUser.doAs((PrivilegedExceptionAction<Boolean>) () ->
                createAndSetPathOwner(targetPath, sourcePath));
    }

    private boolean checkIfPathExist(Path sourcePath, UserGroupInformation proxyUser) throws Exception {
      if (proxyUser == null) {
        return sourcePath.getFileSystem(conf).exists(sourcePath);
      }
      return proxyUser.doAs((PrivilegedExceptionAction<Boolean>) () ->
              sourcePath.getFileSystem(conf).exists(sourcePath));
    }

    private int handleException(Exception e, Path sourcePath, Path targetPath,
                                int currentRetry, UserGroupInformation proxyUser) {
      try {
        LOG.info("Checking if source path " + sourcePath + " is missing for exception ", e);
        if (!checkIfPathExist(sourcePath, proxyUser)) {
          LOG.info("Source path is missing. Ignoring exception.");
          return 0;
        }
      } catch (Exception ex) {
        LOG.warn("Source path missing check failed. ", ex);
      }

      // retry logic only for i/o exception
      if (!(e instanceof IOException)) {
        LOG.error("Unable to copy {} to {}", sourcePath, targetPath, e);
        setException(e);
        return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
      }

      if (currentRetry <= MAX_COPY_RETRY) {
        LOG.warn("Unable to copy {} to {}", sourcePath, targetPath, e);
      } else {
        LOG.error("Unable to copy {} to {} even after retrying for {} time", sourcePath, targetPath, currentRetry, e);
        setException(e);
        return ErrorMsg.REPL_FILE_SYSTEM_OPERATION_RETRY.getErrorCode();
      }

      int sleepTime = FileUtils.getSleepTime(currentRetry);
      LOG.info("Sleep for " + sleepTime + " milliseconds before retry no " + (currentRetry));
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException timerEx) {
        LOG.info("Sleep interrupted", timerEx.getMessage());
      }

      try {
        if (proxyUser == null) {
          proxyUser = Utils.getUGI();
        }
        FileSystem.closeAllForUGI(proxyUser);
      } catch (Exception ex) {
        LOG.warn("Unable to closeAllForUGI for user " + proxyUser, ex);
      }
      return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    }

    @Override
    protected int execute(DriverContext driverContext) {
      String distCpDoAsUser = conf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);

      Path sourcePath = work.fullyQualifiedSourcePath;
      Path targetPath = work.fullyQualifiedTargetPath;
      if (conf.getBoolVar(HiveConf.ConfVars.REPL_ADD_RAW_RESERVED_NAMESPACE)) {
        sourcePath = reservedRawPath(work.fullyQualifiedSourcePath.toUri());
        targetPath = reservedRawPath(work.fullyQualifiedTargetPath.toUri());
      }
      int currentRetry = 0;
      int error = 0;
      UserGroupInformation proxyUser = null;
      while (currentRetry <= MAX_COPY_RETRY) {
        try {
          UserGroupInformation ugi = Utils.getUGI();
          String currentUser = ugi.getShortUserName();
          if (distCpDoAsUser != null && !currentUser.equals(distCpDoAsUser)) {
            proxyUser = UserGroupInformation.createProxyUser(
                    distCpDoAsUser, UserGroupInformation.getLoginUser());
          }

          setTargetPathOwner(targetPath, sourcePath, proxyUser);

          // do we create a new conf and only here provide this additional option so that we get away from
          // differences of data in two location for the same directories ?
          // basically add distcp.options.delete to hiveconf new object ?
          FileUtils.distCp(
              sourcePath.getFileSystem(conf), // source file system
              Collections.singletonList(sourcePath),  // list of source paths
              targetPath,
              false,
              proxyUser,
              conf,
              ShimLoader.getHadoopShims());
          return 0;
        } catch (Exception e) {
          currentRetry++;
          error = handleException(e, sourcePath, targetPath, currentRetry, proxyUser);
          if (error == 0) {
            return 0;
          }
        } finally {
          if (proxyUser != null) {
            try {
              FileSystem.closeAllForUGI(proxyUser);
            } catch (IOException e) {
              LOG.error("Unable to closeAllForUGI for user " + proxyUser, e);
              if (error == 0) {
                setException(e);
                error = ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
              }
              break;
            }
          }
        }
      }
      return error;
    }

    private static Path reservedRawPath(URI uri) {
      return new Path(uri.getScheme(), uri.getAuthority(),
          CopyUtils.RAW_RESERVED_VIRTUAL_PATH + uri.getPath());
    }

    @Override
    public StageType getType() {
      return StageType.COPY;
    }

    @Override
    public String getName() {
      return "DIR_COPY_TASK";
    }

    @Override
    public boolean canExecuteInParallel(){
      return true;
    }
  }

  @Explain(displayName = "HDFS Copy Operator", explainLevels = { Explain.Level.USER,
      Explain.Level.DEFAULT,
      Explain.Level.EXTENDED })
  public static class DirCopyWork implements Serializable {
    private final Path fullyQualifiedSourcePath, fullyQualifiedTargetPath;

    public DirCopyWork(Path fullyQualifiedSourcePath, Path fullyQualifiedTargetPath) {
      this.fullyQualifiedSourcePath = fullyQualifiedSourcePath;
      this.fullyQualifiedTargetPath = fullyQualifiedTargetPath;
    }

    @Override
    public String toString() {
      return "DirCopyWork{" +
          "fullyQualifiedSourcePath=" + fullyQualifiedSourcePath +
          ", fullyQualifiedTargetPath=" + fullyQualifiedTargetPath +
          '}';
    }
  }
}
