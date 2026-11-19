/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.writer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ForwardingMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The position deletes that the DV writers of a merge-on-read DML merge into the DVs they write, by data file location:
 * a DV replaces every position delete of its data file.
 * <p>
 * The job names a temp dir of the query for the target table ({@link #addToJobConf}). Every scan of that table collects
 * the position deletes of the data files it plans while its splits are generated, and writes them into a file of its
 * own in the subdir of the snapshot it reads ({@link #collector}). A writer reads and merges all files of the subdir of
 * the snapshot it writes, once per LLAP daemon ({@link #getFromJobConf}): a scan of another branch or tag, or as of
 * another snapshot, reads other deletes.
 */
public final class RewritableDeletes extends ForwardingMap<String, DeleteFileSet> {
  private static final Logger LOG = LoggerFactory.getLogger(RewritableDeletes.class);

  private static final String LOCATION_PREFIX = "iceberg.mr.rewritable.deletes.";
  private static final String DIR_SUFFIX = "-rewritable-deletes";

  // by data file location; shared by the writers of a daemon, so read-only
  private final Map<String, DeleteFileSet> deletes;

  private RewritableDeletes(Map<String, DeleteFileSet> deletes) {
    this.deletes = Collections.unmodifiableMap(deletes);
  }

  @Override
  protected Map<String, DeleteFileSet> delegate() {
    return deletes;
  }

  /**
   * Names the temp dir that the scans of a table collect its rewritable deletes into, under a key of the query.
   * @param tableName The name of the table in the job's configuration.
   * @param jobConf The job's configuration, with the temp dir of the query.
   */
  public static void addToJobConf(String tableName, JobConf jobConf) {
    String key = key(tableName, jobConf);
    String tmpDir = jobConf.get(DagUtils.TEZ_TMP_DIR_KEY);
    Preconditions.checkState(key != null && tmpDir != null,
        "No query id or temp dir of the query (%s) for the rewritable deletes of %s", DagUtils.TEZ_TMP_DIR_KEY,
        tableName);
    jobConf.set(key, new Path(tmpDir, tableName + DIR_SUFFIX).toString());
  }

  /**
   * Returns a collector for the rewritable deletes of a scan of a table, or null when no DV writer of the job writes
   * the table.
   * @param tableName The name of the table in the scan's configuration.
   * @param snapshot The snapshot the scan reads, null when the table has none.
   * @param conf The scan's configuration.
   */
  public static Collector collector(String tableName, Snapshot snapshot, Configuration conf) {
    String location = location(tableName, snapshot, conf);
    return location != null ? new Collector(location) : null;
  }

  // keyed by the query id: a session cannot set the key of a query, so scans never write where a session names
  private static String key(String tableName, Configuration conf) {
    String queryId = conf.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
    return tableName != null && queryId != null ? LOCATION_PREFIX + queryId + "." + tableName : null;
  }

  // the subdir of the snapshot in the dir of the table
  private static String location(String tableName, Snapshot snapshot, Configuration conf) {
    String key = key(tableName, conf);
    String dir = key != null ? conf.get(key) : null;
    return dir != null ? new Path(dir, String.valueOf(snapshot != null ? snapshot.snapshotId() : -1)).toString() : null;
  }

  /**
   * Returns the rewritable deletes of a snapshot of a table, merged from the files all scans of the snapshot wrote.
   * Fails when the dir holds no file, rather than returning no deletes.
   * @param tableName The name of the table in the task's configuration.
   * @param snapshot The snapshot the task writes, null when the table has none.
   * @param conf The task's configuration.
   * @return The rewritable deletes by data file location; empty when the job writes no DVs to the table.
   */
  public static Map<String, DeleteFileSet> getFromJobConf(String tableName, Snapshot snapshot, Configuration conf) {
    String location = location(tableName, snapshot, conf);
    if (location == null) {
      return Map.of();
    }
    ObjectCache cache =
        ObjectCacheFactory.getCache(conf, conf.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname), false, true);
    if (cache == null) {
      return readDir(location, conf);
    }
    try {
      // the writers keep the cached value itself: the LLAP object cache holds its values softly
      return cache.retrieve(location, () -> readDir(location, conf));
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Collects the position deletes of the data files a scan plans.
   */
  public static final class Collector {
    private final String location;
    private final Map<String, DeleteFileSet> deletes = Maps.newHashMap();

    private Collector(String location) {
      this.location = location;
    }

    /**
     * Adds the position deletes, DVs included, of the task's data file.
     */
    public void add(FileScanTask task) {
      for (DeleteFile delete : task.deletes()) {
        if (delete.content() == FileContent.POSITION_DELETES) {
          deletes.computeIfAbsent(task.file().location(), ignored -> DeleteFileSet.create()).add(delete);
        }
      }
    }

    /**
     * Writes the collected deletes into a new file of the dir. A file is written also when nothing was collected, so a
     * writer can tell a scan without previous deletes from a missing one.
     */
    public void write(Configuration conf) {
      Path path = new Path(location, UUID.randomUUID() + ".gz");
      try (OutputStream out = new GZIPOutputStream(path.getFileSystem(conf).create(path, false))) {
        out.write(SerializationUtil.serializeToBytes(deletes));
      } catch (IOException e) {
        throw new UncheckedIOException(String.format("Failed to write rewritable deletes to %s", path), e);
      }
      LOG.info("Collected rewritable deletes of {} data files: {}", deletes.size(), path);
    }
  }

  private static RewritableDeletes readDir(String location, Configuration conf) {
    LOG.debug("Reading rewritable deletes from {}", location);
    Path dir = new Path(location);
    Map<String, DeleteFileSet> deletes = Maps.newHashMap();
    try {
      FileSystem fs = dir.getFileSystem(conf);
      FileStatus[] files;
      try {
        files = fs.listStatus(dir);
      } catch (FileNotFoundException e) {
        files = new FileStatus[0];
      }
      Preconditions.checkState(files.length > 0, "No scan collected the rewritable deletes into %s", location);
      for (FileStatus file : files) {
        read(fs, file.getPath()).forEach((dataFile, deleteFiles) ->
            deletes.computeIfAbsent(dataFile, ignored -> DeleteFileSet.create()).addAll(deleteFiles));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to read rewritable deletes from %s", location), e);
    }
    return new RewritableDeletes(deletes);
  }

  private static Map<String, DeleteFileSet> read(FileSystem fs, Path file) throws IOException {
    try (InputStream in = new GZIPInputStream(fs.open(file))) {
      return SerializationUtil.deserializeFromBytes(in.readAllBytes());
    }
  }

  /**
   * Returns the files the scans wrote under a dir, with the deletes each holds.
   */
  @VisibleForTesting
  static Map<Path, Map<String, DeleteFileSet>> scanFiles(Path root, Configuration conf) throws IOException {
    Map<Path, Map<String, DeleteFileSet>> scanFiles = Maps.newHashMap();
    FileSystem fs = root.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(root, true);
    while (files.hasNext()) {
      Path file = files.next().getPath();
      if (isScanFile(file)) {
        scanFiles.put(file, read(fs, file));
      }
    }
    return scanFiles;
  }

  // a file in the subdir of a snapshot in the dir of a table, see location
  private static boolean isScanFile(Path file) {
    Path tableDir = file.getParent().getParent();
    return tableDir != null && tableDir.getName().endsWith(DIR_SUFFIX);
  }
}
