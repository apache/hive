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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Carries the previous position deletes of the target table's data files from its scans to the DV writers, which merge
 * them into their DVs.
 * <p>
 * Each scan of the target table writes the deletes of the data files it plans to its own file in a temp dir of the
 * query, with one subdir per snapshot ({@link #collector}). A writer reads the files of the snapshot it writes to, once
 * per LLAP daemon ({@link #getFromJobConf}).
 */
public final class RewritableDeletes {
  private static final Logger LOG = LoggerFactory.getLogger(RewritableDeletes.class);

  private static final String LOCATION_PREFIX = "iceberg.mr.rewritable.deletes.";
  @VisibleForTesting
  static final String DIR_SUFFIX = "-rewritable-deletes";

  private RewritableDeletes() {
  }

  /**
   * Sets the temp dir the scans of the table write their deletes to.
   * @param tableName The table name, as used in the job configuration.
   * @param jobConf The job configuration, which holds the temp dir of the query.
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
   * Returns a collector for a scan of the table, or null if no DV writer of the job writes to the table.
   * @param tableName The table name, as used in the scan configuration.
   * @param snapshot The snapshot the scan reads, or null if the table has no snapshot.
   * @param conf The scan configuration.
   */
  public static Collector collector(String tableName, Snapshot snapshot, Configuration conf) {
    String location = location(tableName, snapshot, conf);
    return location != null ? new Collector(location) : null;
  }

  // includes the query id, so a value set in the session is never used
  private static String key(String tableName, Configuration conf) {
    String queryId = conf.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
    return tableName != null && queryId != null ? LOCATION_PREFIX + queryId + "." + tableName : null;
  }

  // <table dir>/<snapshot id>, or null if no DV writer of the job writes to the table
  private static String location(String tableName, Snapshot snapshot, Configuration conf) {
    String key = key(tableName, conf);
    String dir = key != null ? conf.get(key) : null;
    if (dir == null) {
      return null;
    }
    long snapshotId = snapshot != null ? snapshot.snapshotId() : -1;
    return new Path(dir, String.valueOf(snapshotId)).toString();
  }

  /**
   * Returns the deletes the scans of the snapshot collected. Fails if no scan wrote a file.
   * @param tableName The table name, as used in the task configuration.
   * @param snapshot The snapshot the task writes to, or null if the table has no snapshot.
   * @param conf The task configuration.
   * @return The deletes by data file location; empty if the job writes no DVs to the table.
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
      // the cache holds its values softly; the writers keep the returned map
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
     * Adds the position deletes of the task's data file, including its DV.
     */
    public void add(FileScanTask task) {
      for (DeleteFile delete : task.deletes()) {
        if (delete.content() == FileContent.POSITION_DELETES) {
          deletes.computeIfAbsent(task.file().location(), ignored -> DeleteFileSet.create()).add(delete);
        }
      }
    }

    /**
     * Writes the collected deletes to a new file, also when empty: a missing file means the scan did not collect.
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

  private static Map<String, DeleteFileSet> readDir(String location, Configuration conf) {
    LOG.debug("Reading rewritable deletes from {}", location);
    Path dir = new Path(location);
    Map<String, DeleteFileSet> deletes = Maps.newHashMap();
    try {
      FileSystem fs = dir.getFileSystem(conf);
      // null when no scan created the dir
      FileStatus[] files = Utilities.listStatusIfExists(dir, fs);
      Preconditions.checkState(files != null && files.length > 0,
          "No scan collected the rewritable deletes into %s", location);
      for (FileStatus file : files) {
        read(fs, file.getPath()).forEach((dataFile, deleteFiles) ->
            deletes.computeIfAbsent(dataFile, ignored -> DeleteFileSet.create()).addAll(deleteFiles));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to read rewritable deletes from %s", location), e);
    }
    // read-only: the writers of an LLAP daemon share it
    return Collections.unmodifiableMap(deletes);
  }

  @VisibleForTesting
  static Map<String, DeleteFileSet> read(FileSystem fs, Path file) throws IOException {
    try (InputStream in = new GZIPInputStream(fs.open(file))) {
      return SerializationUtil.deserializeFromBytes(in.readAllBytes());
    }
  }
}
