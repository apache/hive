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

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_SNAPSHOT_OVERWRITE_TARGET_FOR_EXTERNAL_TABLE_COPY;
import static org.apache.hadoop.hive.metastore.utils.HdfsUtils.constructDistCpOptions;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.firstSnapshot;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.secondSnapshot;

/**
 * DirCopyTask, mainly to be used to copy External table data.
 */
public class DirCopyTask extends Task<DirCopyWork> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(DirCopyTask.class);
  private static final String CUSTOM_PATH_CONFIG_PREFIX = "hive.dbpath.";

  private boolean createAndSetPathOwner(Path destPath, Path sourcePath, HiveConf clonedConf) throws IOException {
    FileSystem targetFs = destPath.getFileSystem(clonedConf);
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
      status = sourcePath.getFileSystem(clonedConf).getFileStatus(sourcePath);
    } catch (FileNotFoundException e) {
      // Don't delete target path created else ddl task will try to create it using user hive and may fail.
      LOG.warn("source path missing " + sourcePath);
      return createdDir;
    }
    LOG.info("Setting permission for path dest {} from source {} owner {} : {} : {}",
            destPath, sourcePath, status.getOwner(), status.getGroup(), status.getPermission());
    preserveDistCpAttributes(destPath, sourcePath, clonedConf, status);
    return createdDir;
  }

  private void preserveDistCpAttributes(Path destPath, Path sourcePath, HiveConf clonedConf, FileStatus status)
      throws IOException {
    String preserveString = getPreserveString(clonedConf);
    LOG.info("Preserving DistCp Attributes: {}", preserveString);
    FileSystem destFs = destPath.getFileSystem(clonedConf);
    // If both preserve user and group are specified.
    if (preserveString.contains("u") && preserveString.contains("g")) {
      destFs.setOwner(destPath, status.getOwner(), status.getGroup());
    } else if (preserveString.contains("u")) {
      // If only preserve user is specified.
      destFs.setOwner(destPath, status.getOwner(), null);
    } else if (preserveString.contains("g")) {
      // If only preserve group is specified.
      destFs.setOwner(destPath, null, status.getGroup());
    }
    // Preserve permissions
    if (preserveString.contains("p")) {
      destFs.setPermission(destPath, status.getPermission());
    }
    // Preserve ACL
    if (preserveString.contains("a")) {
      setAclsToTarget(status, sourcePath, destPath, clonedConf);
    }
  }

  private void setAclsToTarget(FileStatus sourceStatus, Path sourcePath, Path destPath, HiveConf clonedConf)
      throws IOException {
    // Check if distCp options contains preserve ACL.
    AclStatus sourceAcls = sourcePath.getFileSystem(clonedConf).getAclStatus(sourcePath);
    if (sourceAcls != null && sourceAcls.getEntries().size() > 0) {
      destPath.getFileSystem(clonedConf).removeAcl(destPath);
      List<AclEntry> effectiveAclEntries =
          AclUtil.getAclFromPermAndEntries(sourceStatus.getPermission(), sourceAcls.getEntries());
      destPath.getFileSystem(clonedConf).setAcl(destPath, effectiveAclEntries);
    }
  }

  private String getPreserveString(HiveConf clonedConf) {
    List<String> distCpOptions = constructDistCpOptions(clonedConf);
    for (String option : distCpOptions) {
      if (option.startsWith("-p")) {

        return option.replaceFirst("-p", "");
      }
    }
    return "";
  }

  private boolean setTargetPathOwner(Path targetPath, Path sourcePath, UserGroupInformation proxyUser,
      HiveConf clonedConf)
            throws IOException, InterruptedException {
    if (proxyUser == null) {
      return createAndSetPathOwner(targetPath, sourcePath, clonedConf);
    }
    return proxyUser.doAs((PrivilegedExceptionAction<Boolean>) () -> createAndSetPathOwner(targetPath, sourcePath,
        clonedConf));
  }

  private boolean checkIfPathExist(Path sourcePath, UserGroupInformation proxyUser, HiveConf clonedConf) throws Exception {
    if (proxyUser == null) {
      return sourcePath.getFileSystem(clonedConf).exists(sourcePath);
    }
    return proxyUser.doAs((PrivilegedExceptionAction<Boolean>) () -> sourcePath.getFileSystem(clonedConf).exists(sourcePath));
  }

  @Override
  public int execute() {
    LOG.info("Started DirCopyTask for table {} from source: {} to target: {}", work.getTableName(), work.getFullyQualifiedSourcePath(),
        work.getFullyQualifiedTargetPath());
    HiveConf clonedConf = getConf(conf);
    String distCpDoAsUser = clonedConf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
    Retryable retryable = Retryable.builder()
      .withHiveConf(clonedConf)
      .withRetryOnException(IOException.class).withFailOnException(SnapshotException.class).build();
    long startTime = System.currentTimeMillis();
    AtomicInteger retries = new AtomicInteger(-1);
    AtomicBoolean result = new AtomicBoolean(false);
    try {
      return retryable.executeCallable(() -> {
        retries.getAndIncrement();
        UserGroupInformation proxyUser = null;
        Path sourcePath = work.getFullyQualifiedSourcePath();
        Path targetPath = work.getFullyQualifiedTargetPath();
        try {
          if (clonedConf.getBoolVar(HiveConf.ConfVars.REPL_ADD_RAW_RESERVED_NAMESPACE)) {
            sourcePath = reservedRawPath(work.getFullyQualifiedSourcePath().toUri());
            targetPath = reservedRawPath(work.getFullyQualifiedTargetPath().toUri());
          }
          UserGroupInformation ugi = Utils.getUGI();
          String currentUser = ugi.getShortUserName();
          if (distCpDoAsUser != null && !currentUser.equals(distCpDoAsUser)) {
            proxyUser = UserGroupInformation.createProxyUser(
              distCpDoAsUser, UserGroupInformation.getLoginUser());
          }

          setTargetPathOwner(targetPath, sourcePath, proxyUser, clonedConf);
          try {
            if (!checkIfPathExist(sourcePath, proxyUser, clonedConf)) {
              LOG.info("Source path is missing. Ignoring exception.");
              return 0;
            }
          } catch (Exception ex) {
            LOG.warn("Source path missing check failed. ", ex);
            //Should be retried
            throw new IOException(ex);
          }
          if (!getWork().getCopyMode().equals(SnapshotUtils.SnapshotCopyMode.FALLBACK_COPY)) {
            LOG.info("Using Snapshot mode of copy for source: {} and target: {}", sourcePath, targetPath);
            // Use distcp with snapshots for copy.
            result.set(copyUsingDistCpSnapshots(sourcePath, targetPath, proxyUser, clonedConf));
          } else {
            LOG.info("Using Normal copy for source: {} and target: {}", sourcePath, targetPath);
            result.set(runFallbackDistCp(sourcePath, targetPath, proxyUser, clonedConf));
          }
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
      return ReplUtils.handleException(true, ex, work.getDumpDirectory(), work.getMetricCollector(), getName(), clonedConf);
    } finally {
      String jobId = clonedConf.get(ReplUtils.DISTCP_JOB_ID_CONF, ReplUtils.DISTCP_JOB_ID_CONF_DEFAULT);
      LOG.info("DirCopyTask status for source: {} to  target: {}. Took {}. DistCp JobId {}. Number of retries {}. "
              + "Result: {}", work.getFullyQualifiedSourcePath(), work.getFullyQualifiedTargetPath(),
          ReplUtils.convertToHumanReadableTime(System.currentTimeMillis() - startTime), jobId, retries.get(),
          result.get() ? "SUCCEEDED" : "FAILED");

      // if distcp succeeded then update bytes copied counter
      if (result.get()) {
        FileSystem srcFs = null;
        try {
          srcFs = work.getFullyQualifiedSourcePath().getFileSystem(clonedConf);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        ContentSummary summary = null;
        try {
          summary = srcFs.getContentSummary(work.getFullyQualifiedSourcePath());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        final long totalBytesCopied = summary.getLength();
        LOG.debug("DirCopyTask copied {} number of bytes by using distcp", totalBytesCopied);
        // increment total bytes replicated counter
        if (work.getMetricCollector() != null) {
          work.getMetricCollector().incrementSizeOfDataReplicated(totalBytesCopied);
        }
      }
    }
  }

  private HiveConf getConf(HiveConf conf) {
    // if it is a db level path check for custom configurations.
    HiveConf clonedConf = new HiveConf(conf);
    if (work.getTableName().startsWith("dbPath:")) {
      for (Map.Entry<String, String> entry : conf.getPropsWithPrefix(CUSTOM_PATH_CONFIG_PREFIX).entrySet()) {
        clonedConf.set(entry.getKey().replaceFirst(CUSTOM_PATH_CONFIG_PREFIX, ""), entry.getValue());
      }
    }
    return clonedConf;
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

  boolean copyUsingDistCpSnapshots(Path sourcePath, Path targetPath, UserGroupInformation proxyUser,
      HiveConf clonedConf) throws IOException {

    DistributedFileSystem targetFs = SnapshotUtils.getDFS(targetPath, clonedConf);
    boolean result = false;
    if (getWork().getCopyMode().equals(SnapshotUtils.SnapshotCopyMode.DIFF_COPY)) {
      LOG.info("Using snapshot diff copy for source: {} and target: {}", sourcePath, targetPath);
      boolean overwriteTarget = clonedConf.getBoolVar(REPL_SNAPSHOT_OVERWRITE_TARGET_FOR_EXTERNAL_TABLE_COPY);
      LOG.debug("Overwrite target in case the target location is modified is turned {}",
          overwriteTarget ? "on" : "off");
      result = FileUtils
          .distCpWithSnapshot(firstSnapshot(work.getSnapshotPrefix()), secondSnapshot(work.getSnapshotPrefix()),
              Collections.singletonList(sourcePath), targetPath, overwriteTarget, clonedConf,
              ShimLoader.getHadoopShims(), proxyUser);
       if(result) {
         // Delete the older snapshot from last iteration.
         targetFs.deleteSnapshot(targetPath, firstSnapshot(work.getSnapshotPrefix()));
       } else {
         throw new SnapshotException(
             "Can not successfully copy external table data using snapshot diff. source: " + sourcePath + " and "
                 + "target: " + targetPath);
       }
    } else if (getWork().getCopyMode().equals(SnapshotUtils.SnapshotCopyMode.INITIAL_COPY)) {
      LOG.info("Using snapshot initial copy for source: {} and target: {}", sourcePath, targetPath);
      // Get the path relative to the initial snapshot for copy.
      Path snapRelPath =
          new Path(sourcePath, HdfsConstants.DOT_SNAPSHOT_DIR + "/" + secondSnapshot(work.getSnapshotPrefix()));

      // This is the first time we are copying, check if the target is snapshottable or not, if not attempt to allow
      // snapshots.
      SnapshotUtils.allowSnapshot(targetFs, work.getFullyQualifiedTargetPath(), clonedConf);
      // Attempt to delete the snapshot, in case this is a bootstrap post a failed incremental, Since in case of
      // bootstrap we go from start, so delete any pre-existing snapshot.
      SnapshotUtils.deleteSnapshotIfExists(targetFs, targetPath, firstSnapshot(work.getSnapshotPrefix()), clonedConf);

      // Copy from the initial snapshot path.
      result = runFallbackDistCp(snapRelPath, targetPath, proxyUser, clonedConf);
    }

    // Create a new snapshot at target Filesystem. For the next iteration.
    if (result) {
      SnapshotUtils.createSnapshot(targetFs, targetPath, firstSnapshot(work.getSnapshotPrefix()), clonedConf);
    }
    return result;
  }

  private boolean runFallbackDistCp(Path sourcePath, Path targetPath, UserGroupInformation proxyUser,
      HiveConf clonedConf)
      throws IOException {
     // do we create a new conf and only here provide this additional option so that we get away from
    // differences of data in two location for the same directories ?
    // basically add distcp.options.delete to hiveconf new object ?
    boolean response = FileUtils.distCp(
        sourcePath.getFileSystem(clonedConf), // source file system
        Collections.singletonList(sourcePath),  // list of source paths
        targetPath,
        false,
        proxyUser,
        clonedConf,
        ShimLoader.getHadoopShims());
    return response;
  }
}
