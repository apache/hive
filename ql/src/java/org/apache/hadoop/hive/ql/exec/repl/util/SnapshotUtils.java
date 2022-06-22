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
package org.apache.hadoop.hive.ql.exec.repl.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.ql.exec.repl.ReplDumpTask.createTableFileList;
import static org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables.externalTableDataPath;
import static org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables.getExternalTableBaseDir;

/**
 * Utility class for snapshot related operations.
 */
public class SnapshotUtils {

  private static final transient Logger LOG = LoggerFactory.getLogger(SnapshotUtils.class);

  public static final String OLD_SNAPSHOT = "replOld";
  public static final String NEW_SNAPSHOT = "replNew";

  /**
   * Gets a DistributedFileSystem object if possible from a path.
   * @param path path from which DistributedFileSystem needs to be extracted.
   * @param conf Hive Configuration.
   * @return DFS or null.
   */
  public static DistributedFileSystem getDFS(Path path, HiveConf conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs instanceof DistributedFileSystem) {
      return (DistributedFileSystem) fs;
    } else {
      LOG.error("FileSystem for {} is not DistributedFileSystem", path);
      throw new IOException("The filesystem for path {} is {}, The filesystem should be DistributedFileSystem to "
          + "support snapshot based copy.");
    }
  }

  /**
   *  Checks whether a given snapshot exists or not.
   * @param dfs DistributedFileSystem.
   * @param path path of snapshot.
   * @param snapshotPrefix snapshot name prefix.
   * @param snapshotName name of snapshot.
   * @param conf Hive configuration.
   * @return true if the snapshot exists.
   * @throws IOException in case of any error.
   */
  public static boolean isSnapshotAvailable(DistributedFileSystem dfs, Path path, String snapshotPrefix,
      String snapshotName, HiveConf conf) throws IOException {
    AtomicBoolean isSnapAvlb = new AtomicBoolean(false);
    Retryable retryable = Retryable.builder().withHiveConf(conf).withRetryOnException(IOException.class)
        .withFailOnException(SnapshotException.class).build();
    try {
      retryable.executeCallable(() -> {
        isSnapAvlb
            .set(dfs.exists(new Path(path, HdfsConstants.DOT_SNAPSHOT_DIR + "/" + snapshotPrefix + snapshotName)));
        LOG.debug("Snapshot for path {} is {}", path, isSnapAvlb.get() ? "available" : "unavailable");
        return null;
      });
    } catch (Exception e) {
      throw new SnapshotException("Failed to check if snapshot is available on " + path, e);
    }
    return isSnapAvlb.get();
  }

  /**
   * Attempts deleting a snapshot, if exists.
   * @param dfs DistributedFileSystem
   * @param snapshotPath path of snapshot
   * @param snapshotName name of the snapshot
   * @return true if the snapshot gets deleted.
   */
  public static boolean deleteSnapshotSafe(DistributedFileSystem dfs, Path snapshotPath, String snapshotName)
      throws IOException {
    try {
      dfs.deleteSnapshot(snapshotPath, snapshotName);
      return true;
    } catch (SnapshotException e) {
      LOG.debug("Couldn't delete the snapshot {} under path {}", snapshotName, snapshotPath, e);
    } catch (FileNotFoundException fnf) {
      LOG.warn("Couldn't delete the snapshot {} under path {}", snapshotName, snapshotPath, fnf);
    }
    return false;
  }

  /**
   *  Attempts deleting a snapshot if it exists, with retries.
   * @param fs FileSystem object.
   * @param snapshotPath Path of the snapshot.
   * @param snapshotName Name of the snapshot.
   * @param conf Hive Configuration.
   * @return true if snapshot is no longer available.
   */
  public static boolean deleteSnapshotIfExists(DistributedFileSystem fs, Path snapshotPath, String snapshotName,
      HiveConf conf) throws IOException {
    Retryable retryable = Retryable.builder().withHiveConf(conf).withRetryOnException(IOException.class)
        .withFailOnException(SnapshotException.class).build();
    try {
      retryable.executeCallable(() -> {
        try {
          if (fs.exists(new Path(snapshotPath, ".snapshot/" + snapshotName))) {
            fs.deleteSnapshot(snapshotPath, snapshotName);
          }
        } catch (FileNotFoundException e) {
          // Ignore FileNotFoundException, Our intention is to make sure the snapshot doesn't exist and the FNF means
          // it already doesn't exist.
          LOG.warn("Couldn't create the snapshot {} under path {}. It doesn't exist", snapshotName, snapshotPath, e);
        } catch (SnapshotException e) {
          if (e.getMessage().contains("the snapshot does not exist") || e.getMessage()
              .contains("Directory is not a snapshottable directory")) {
            return true;
          }
        }
        return null;
      });
    } catch (Exception e) {
      throw new SnapshotException(
          "Unable to delete snapshot for path: " + snapshotPath + " snapshot name: " + snapshotName, e);
    }
    return true;
  }

  /**
   *  Attempts to disallow snapshot on a path, ignoring exceptions.
   * @param dfs DistributedFileSystem
   * @param snapshotPath path of snapshot
   */
  public static void disallowSnapshot(DistributedFileSystem dfs, Path snapshotPath) {
    try {
      // Check if the directory is snapshottable.
      if (dfs.getFileStatus(snapshotPath).isSnapshotEnabled()) {
        dfs.disallowSnapshot(snapshotPath);
      }
    } catch (Exception e) {
      LOG.warn("Could not disallow snapshot for path {}", snapshotPath, e);
    }
  }

  /**
   * Attempts to allow snapshots on the path, with retry.
   * @param dfs DistributedFileSystem.
   * @param snapshotPath path of snapshot.
   * @param conf Hive Configuration.
   */
  public static void allowSnapshot(DistributedFileSystem dfs, Path snapshotPath, HiveConf conf) throws IOException {
    // Check if the directory is already snapshottable.
    Retryable retryable = Retryable.builder().withHiveConf(conf).withRetryOnException(IOException.class)
        .withFailOnException(SnapshotException.class).build();
    try {
      retryable.executeCallable(() -> {
        try {
          if (!dfs.getFileStatus(snapshotPath).isSnapshotEnabled()) {
            dfs.allowSnapshot(snapshotPath);
          }
        } catch (FileNotFoundException fnf) {
          // Source got deleted, we can ignore.
          LOG.info("Failed to allow snapshot for {} since the path got deleted", snapshotPath);
        }
        return null;
      });
    } catch (Exception e) {
      throw new SnapshotException("Failed to AllowSnapshot on " + snapshotPath, e);
    }
  }

  /**
   * Specifies the mode of copy when using Snapshots for replication.
   */
  public enum SnapshotCopyMode {
    INITIAL_COPY, // Copying for the first time, only one snapshot exists.
    DIFF_COPY, // Copying when a pair of snapshots are present
    FALLBACK_COPY // Symbolizes that normal copy needs to be used, That is without use of snapshots.
  }

  /**
   *  Attempts creating a snapshot, with retries.
   * @param fs FileSystem object.
   * @param snapshotPath Path of the snapshot.
   * @param snapshotName Name of the snapshot.
   * @param conf Hive Configuration.
   */
  public static void createSnapshot(FileSystem fs, Path snapshotPath, String snapshotName, HiveConf conf)
      throws IOException {
    Retryable retryable = Retryable.builder().withHiveConf(conf).withRetryOnException(IOException.class)
        .withFailOnException(SnapshotException.class).build();
    try {
      retryable.executeCallable(() -> {
        try {
          fs.createSnapshot(snapshotPath, snapshotName);
        } catch (FileNotFoundException e) {
          LOG.warn("Couldn't create the snapshot {} under path {}", snapshotName, snapshotPath, e);
        }
        return null;
      });
    } catch (Exception e) {
      throw new SnapshotException(
          "Unable to create snapshot for path: " + snapshotPath + " snapshot name: " + snapshotName, e);
    }
  }

  /**
   * Renames a snapshot with retries.
   * @param fs the filesystem.
   * @param snapshotPath the path where the snapshot lies.
   * @param sourceSnapshotName current snapshot name.
   * @param targetSnapshotName new snapshot name.
   * @param conf configuration.
   * @throws IOException in case of failure.
   */
  public static void renameSnapshot(FileSystem fs, Path snapshotPath, String sourceSnapshotName,
      String targetSnapshotName, HiveConf conf) throws IOException {
    Retryable retryable = Retryable.builder().withHiveConf(conf).withRetryOnException(IOException.class)
        .withFailOnException(SnapshotException.class).build();
    try {
      retryable.executeCallable(() -> {
        try {
          fs.renameSnapshot(snapshotPath, sourceSnapshotName, targetSnapshotName);
        } catch (FileNotFoundException e) {
          LOG.warn("Couldn't rename the snapshot {} to {} under path {}", sourceSnapshotName, targetSnapshotName,
              snapshotPath, e);
        }
        return null;
      });
    } catch (Exception e) {
      throw new SnapshotException(
          "Unable to rename snapshot " + sourceSnapshotName + " to " + targetSnapshotName + " for path: "
              + snapshotPath, e);
    }
  }

  /**
   * Extracts the entries from FileList into an ArrayList
   * @param fl the FileList
   * @return the ArrayList containing the entries.
   */
  public static ArrayList<String> getListFromFileList(FileList fl) {
    ArrayList<String> result = new ArrayList<>();
    while (fl.hasNext()) {
      result.add(fl.next());
    }
    return result;
  }

  /**
   *  Deletes the snapshots present in the list.
   * @param diffList Elements to be deleted.
   * @param prefix Prefix used in snapshot names,
   * @param snapshotCount snapshot counter to track the number of snapshots deleted.
   * @param conf the Hive Configuration.
   * @throws IOException in case of any error.
   */
  private static void cleanUpSnapshots(ArrayList<String> diffList, String prefix, ReplSnapshotCount snapshotCount,
      HiveConf conf) throws IOException {
    DistributedFileSystem dfs =
        diffList.size() > 0 ? (DistributedFileSystem) new Path(diffList.get(0)).getFileSystem(conf) : null;
    for (String path : diffList) {
      Path snapshotPath = new Path(path);
      boolean isFirstDeleted = deleteSnapshotIfExists(dfs, snapshotPath, firstSnapshot(prefix), conf);
      boolean isSecondDeleted = deleteSnapshotIfExists(dfs, snapshotPath, secondSnapshot(prefix), conf);
      // Only attempt to disallowSnapshot, we have deleted the snapshots related to our replication policy, so if
      // this path was only used by this policy for snapshots, then the disallow will be success, if it is used by
      // some other applications as well, it won't be success, in that case we ignore.
      disallowSnapshot(dfs, snapshotPath);
      if (snapshotCount != null) {
        if (isFirstDeleted) {
          snapshotCount.incrementNumDeleted();
        }
        if (isSecondDeleted) {
          snapshotCount.incrementNumDeleted();
        }
      }
    }
  }

  private static ArrayList<String> getDiffList(ArrayList<String> newList, ArrayList<String> oldList, HiveConf conf,
      boolean isLoad) throws SemanticException {
    ArrayList<String> diffList = new ArrayList<>();
    for (String oldPath : oldList) {
      if (!newList.contains(oldPath)) {
        if (isLoad) {
          diffList.add(externalTableDataPath(conf, getExternalTableBaseDir(conf), new Path(oldPath)).toString());
        } else {
          diffList.add(oldPath);
        }
        diffList.add(oldPath);
      }
    }
    return diffList;
  }

  /**
   * Cleans up any snapshots by computing diff between the list of snapshots between two replication dumps.
   * @param dumpRoot Root of the dump
   * @param snapshotPrefix prefix used by the snapshot
   * @param conf Hive Configuration.
   * @param snapshotCount the counter to store number of deleted snapshots.
   * @param isLoad If this is called for clearing up snapshots at target cluster, In case of load it renames the
   *               snapshot file as well.
   * @throws IOException
   * @throws SemanticException
   */
  public static void cleanupSnapshots(Path dumpRoot, String snapshotPrefix, HiveConf conf,
      ReplSnapshotCount snapshotCount, boolean isLoad) throws IOException, SemanticException {
    DistributedFileSystem dumpDfs = (DistributedFileSystem) dumpRoot.getFileSystem(conf);
    if (dumpDfs.exists(new Path(dumpRoot, EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_OLD))) {
      FileList snapOld = createTableFileList(dumpRoot, EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_OLD, conf);
      FileList snapNew = createTableFileList(dumpRoot, EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_CURRENT, conf);
      ArrayList<String> oldPaths = SnapshotUtils.getListFromFileList(snapOld);
      ArrayList<String> newPaths = SnapshotUtils.getListFromFileList(snapNew);
      ArrayList<String> diffList = SnapshotUtils.getDiffList(newPaths, oldPaths, conf, isLoad);
      SnapshotUtils.cleanUpSnapshots(diffList, snapshotPrefix, snapshotCount, conf);
    }
    if (isLoad) {
      try {
        dumpDfs.delete((new Path(dumpRoot, EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_OLD)), true);
      } catch (FileNotFoundException fnf) {
        // ignore
        LOG.warn("Failed to clean up snapshot " + EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_OLD, fnf);
      }
      try {
        dumpDfs.rename(new Path(dumpRoot, EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_CURRENT),
            new Path(dumpRoot, EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_OLD), Options.Rename.OVERWRITE);
      } catch (FileNotFoundException fnf) {
        // ignore
        LOG.warn("Failed to clean up snapshot " + EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_CURRENT, fnf);
      }
    }
  }

  /**
   * Gets the path where the snapshotfile needs to be created. The hrepl directory.
   * @param dumpRoot the dumproot.
   * @return the path where the snapshot tracking file is to be created.
   */
  public static Path getSnapshotFileListPath(Path dumpRoot) {
    return dumpRoot.getParent().getParent().getParent();
  }

  /**
   * Gets the name of the first snapshot, combined with the prefix.
   * @param prefix the prefix that needs to be attached to the default hive replication first snapshot name.
   * @return name to be used as first snapshot.
   */
  public static String firstSnapshot(String prefix) {
    return prefix + OLD_SNAPSHOT;
  }

  /**
   * Gets the name of the second snapshot, combined with the prefix.
   * @param prefix the prefix that needs to be attached to the default hive replication second snapshot name.
   * @return name to be used as second snapshot.
   */
  public static String secondSnapshot(String prefix) {
    return prefix + NEW_SNAPSHOT;
  }

  /**
   * Snapshot count holder.
   */
  public static class ReplSnapshotCount {

    AtomicInteger numCreated = new AtomicInteger(0);
    AtomicInteger numDeleted = new AtomicInteger(0);

    public void incrementNumCreated() {
      numCreated.incrementAndGet();
    }

    public void incrementNumDeleted() {
      numDeleted.incrementAndGet();
    }

    public int getNumCreated() {
      return numCreated.get();
    }

    public int getNumDeleted() {
      return numCreated.get();
    }

    @Override
    public String toString() {
      return "ReplSnapshotCount{" + "numCreated=" + numCreated + ", numDeleted=" + numDeleted + '}';
    }
  }
}
