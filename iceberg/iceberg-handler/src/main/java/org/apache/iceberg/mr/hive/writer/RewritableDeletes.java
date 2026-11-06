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
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ForwardingMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The previous deletes that the DV writers of a merge-on-read DML merge into the DVs they write: a DV replaces every
 * position delete of its data file. They reach the writers in a temp file of the query that the job's configuration
 * names, read once per LLAP daemon.
 */
public final class RewritableDeletes extends ForwardingMap<String, DeleteFileSet> {
  private static final Logger LOG = LoggerFactory.getLogger(RewritableDeletes.class);

  private static final String LOCATION_PREFIX = "iceberg.mr.rewritable.deletes.";

  // by data file location
  private final Map<String, DeleteFileSet> deletes;

  private RewritableDeletes(Map<String, DeleteFileSet> deletes) {
    this.deletes = deletes;
  }

  @Override
  protected Map<String, DeleteFileSet> delegate() {
    return deletes;
  }

  /**
   * Adds the rewritable deletes of a table to the job, if there are any.
   * @param table The table the DML writes.
   * @param tableName The name of the table in the job's configuration.
   * @param ref The branch the DML writes.
   * @param jobConf The job's configuration, with the temp dir of the query.
   */
  public static void addToJobConf(Table table, String tableName, String ref, JobConf jobConf) {
    String key = LOCATION_PREFIX + tableName;
    if (jobConf.get(key) != null) {
      return;
    }
    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, ref);
    if (snapshot == null) {
      return;
    }
    TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId());
    Map<String, DeleteFileSet> deletes = rewritableDeletes(scan);
    if (deletes.isEmpty()) {
      return;
    }
    Path path = new Path(jobConf.get(DagUtils.TEZ_TMP_DIR_KEY), tableName + "-rewritable-deletes");
    try (OutputStream out = new GZIPOutputStream(path.getFileSystem(jobConf).create(path))) {
      out.write(SerializationUtil.serializeToBytes(deletes));
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write rewritable deletes to %s", path), e);
    }
    jobConf.set(key, path.toString());
    LOG.info("Added rewritable deletes of {} data files to the job: {}", deletes.size(), path);
  }

  /**
   * Returns the rewritable deletes of a table that the job carries.
   * @param tableName The name of the table in the task's configuration.
   * @param conf The task's configuration.
   * @return The rewritable deletes by data file location.
   */
  public static Map<String, DeleteFileSet> getFromJobConf(String tableName, Configuration conf) {
    String location = conf.get(LOCATION_PREFIX + tableName);
    if (location == null) {
      return Map.of();
    }
    ObjectCache cache =
        ObjectCacheFactory.getCache(conf, conf.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname), false, true);
    if (cache == null) {
      return readFile(location, conf);
    }
    try {
      // the writers keep the cached value itself: the LLAP object cache holds its values softly
      return cache.retrieve(location, () -> readFile(location, conf));
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  // a DV replaces every position delete of its data file
  private static Map<String, DeleteFileSet> rewritableDeletes(TableScan scan) {
    Map<String, DeleteFileSet> deletes = Maps.newHashMap();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() == FileContent.POSITION_DELETES) {
            deletes.computeIfAbsent(task.file().location(), ignored -> DeleteFileSet.create()).add(delete);
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to close table scan: %s", scan), e);
    }
    return deletes;
  }

  private static RewritableDeletes readFile(String location, Configuration conf) {
    LOG.info("Reading rewritable deletes from {}", location);
    Path path = new Path(location);
    try (InputStream in = new GZIPInputStream(path.getFileSystem(conf).open(path))) {
      return new RewritableDeletes(SerializationUtil.deserializeFromBytes(in.readAllBytes()));
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to read rewritable deletes from %s", location), e);
    }
  }

  // the object cache of a Tez container logs the values it returns
  @Override
  public String toString() {
    return "rewritable deletes of " + deletes.size() + " data files";
  }
}
