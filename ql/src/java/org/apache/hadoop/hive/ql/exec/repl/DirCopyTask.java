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
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.parse.repl.CopyUtils;
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
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.utils.HdfsUtils.constructDistCpOptions;

/**
 * DirCopyTask, mainly to be used to copy External table data.
 */
public class DirCopyTask extends Task<DirCopyWork> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(DirCopyTask.class);

  private boolean createAndSetPathOwner(Path destPath, Path sourcePath) throws IOException {
    FileSystem targetFs = destPath.getFileSystem(conf);
    boolean createdDir = false;
    if (!targetFs.exists(destPath)) {
      // target path is created even if the source path is missing, so that ddl task does not try to create it.
      if (!targetFs.mkdirs(destPath)) {
        throw new IOException(ErrorMsg.REPL_FILE_SYSTEM_OPERATION_RETRY.format(
          destPath + " is not a directory or unable to create one"));
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
    setAclsToTarget(status, sourcePath, destPath);
    return createdDir;
  }

  private void setAclsToTarget(FileStatus sourceStatus, Path sourcePath,
      Path destPath) throws IOException {
    // Check if distCp options contains preserve ACL.
    if (isPreserveAcl()) {
      AclStatus sourceAcls =
          sourcePath.getFileSystem(conf).getAclStatus(sourcePath);
      if (sourceAcls != null && sourceAcls.getEntries().size() > 0) {
        destPath.getFileSystem(conf).removeAcl(destPath);
        List<AclEntry> effectiveAclEntries = AclUtil
            .getAclFromPermAndEntries(sourceStatus.getPermission(),
                sourceAcls.getEntries());
        destPath.getFileSystem(conf).setAcl(destPath, effectiveAclEntries);
      }
    }
  }

  private boolean isPreserveAcl() {
    List<String> distCpOptions = constructDistCpOptions(conf);
    for (String option : distCpOptions) {
      if (option.startsWith("-p")) {
        if (option.contains("a")) {
          return true;
        } else {
          return false;
        }
      }
    }
    return false;
  }

  private boolean setTargetPathOwner(Path targetPath, Path sourcePath, UserGroupInformation proxyUser)
            throws IOException, InterruptedException {
    if (proxyUser == null) {
      return createAndSetPathOwner(targetPath, sourcePath);
    }
    return proxyUser.doAs((PrivilegedExceptionAction<Boolean>) () -> createAndSetPathOwner(targetPath, sourcePath));
  }

  private boolean checkIfPathExist(Path sourcePath, UserGroupInformation proxyUser) throws Exception {
    if (proxyUser == null) {
      return sourcePath.getFileSystem(conf).exists(sourcePath);
    }
    return proxyUser.doAs((PrivilegedExceptionAction<Boolean>) () -> sourcePath.getFileSystem(conf).exists(sourcePath));
  }

  @Override
  public int execute() {
    String distCpDoAsUser = conf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
    Retryable retryable = Retryable.builder()
      .withHiveConf(conf)
      .withRetryOnException(IOException.class).build();
    try {
      return retryable.executeCallable(() -> {
        UserGroupInformation proxyUser = null;
        Path sourcePath = work.getFullyQualifiedSourcePath();
        Path targetPath = work.getFullyQualifiedTargetPath();
        try {
          if (conf.getBoolVar(HiveConf.ConfVars.REPL_ADD_RAW_RESERVED_NAMESPACE)) {
            sourcePath = reservedRawPath(work.getFullyQualifiedSourcePath().toUri());
            targetPath = reservedRawPath(work.getFullyQualifiedTargetPath().toUri());
          }
          UserGroupInformation ugi = Utils.getUGI();
          String currentUser = ugi.getShortUserName();
          if (distCpDoAsUser != null && !currentUser.equals(distCpDoAsUser)) {
            proxyUser = UserGroupInformation.createProxyUser(
              distCpDoAsUser, UserGroupInformation.getLoginUser());
          }

          setTargetPathOwner(targetPath, sourcePath, proxyUser);
          try {
            if (!checkIfPathExist(sourcePath, proxyUser)) {
              LOG.info("Source path is missing. Ignoring exception.");
              return 0;
            }
          } catch (Exception ex) {
            LOG.warn("Source path missing check failed. ", ex);
            //Should be retried
            throw new IOException(ex);
          }
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
        } finally {
          if (proxyUser != null) {
            FileSystem.closeAllForUGI(proxyUser);
          }
        }
      });
    } catch (Exception e) {
      LOG.error("Replication failed ", e);
      Exception ex = new SecurityException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()), e);
      setException(ex);
      return ReplUtils.handleException(true, ex, work.getDumpDirectory(), work.getMetricCollector(), getName(), conf);
    }
  }

  private static Path reservedRawPath(URI uri) {
    return new Path(uri.getScheme(), uri.getAuthority(), CopyUtils.RAW_RESERVED_VIRTUAL_PATH + uri.getPath());
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
  public boolean canExecuteInParallel() {
    return true;
  }
}
