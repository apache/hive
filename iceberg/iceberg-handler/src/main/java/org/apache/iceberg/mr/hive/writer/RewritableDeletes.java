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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DeleteFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The previous deletes that the DV writers of a merge-on-read DML merge into the DVs they write: a DV replaces every
 * position delete of its data file. They are planned while the job is built, into a temp file of the table named
 * after the query. The writers of a query load it once per LLAP daemon.
 */
public final class RewritableDeletes {
  private static final Logger LOG = LoggerFactory.getLogger(RewritableDeletes.class);

  private RewritableDeletes() {
  }

  /**
   * Plans the rewritable deletes of the DV writers of a table.
   * @param table The table the DML writes.
   * @param ref The branch the DML writes.
   * @param conf The job's configuration.
   */
  public static void plan(Table table, String ref, Configuration conf) {
    Long snapshotId = snapshotId(table, ref);
    if (snapshotId == null) {
      return;
    }
    BatchScan scan = table.newBatchScan().useSnapshot(snapshotId);
    Map<String, DeleteFileSet> deletes = rewritableDeletes(scan);
    String location = location(table, conf);
    try (ObjectOutputStream out = new ObjectOutputStream(
        new GZIPOutputStream(table.io().newOutputFile(location).createOrOverwrite()))) {
      out.writeObject(deletes);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write rewritable deletes to %s", location), e);
    }
    LOG.info("Planned rewritable deletes of {} data files to {}", deletes.size(), location);
  }

  /**
   * Loads the rewritable deletes planned for the DV writers of a table.
   * @param table The table the DML writes.
   * @param ref The branch the DML writes.
   * @param conf The task's configuration.
   * @return The rewritable deletes by data file location.
   */
  public static Map<String, DeleteFileSet> load(Table table, String ref, Configuration conf) {
    Long snapshotId = snapshotId(table, ref);
    if (snapshotId == null) {
      return Map.of();
    }
    String location = location(table, conf);
    ObjectCache cache = ObjectCacheFactory.getCache(conf, queryId(conf), false, true);
    if (cache == null) {
      return read(table, location);
    }
    try {
      // a re-execution of the query plans the location again, on the snapshot it reads
      return cache.<Cached>retrieve(location + "@" + snapshotId, () -> new Cached(read(table, location))).deletes();
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Removes the rewritable deletes planned for the DV writers of a table, once the job is committed or aborted.
   * @param table The table the DML writes.
   * @param conf The job's configuration.
   */
  public static void cleanup(Table table, Configuration conf) {
    if (TableUtil.formatVersion(table) < 3) {
      return;
    }
    String location = location(table, conf);
    try {
      table.io().deleteFile(location);
    } catch (RuntimeException e) {
      LOG.warn("Failed to remove the rewritable deletes {}", location, e);
    }
  }

  // the head of the branch, from the refs a serialized table carries without reading the table metadata
  private static Long snapshotId(Table table, String ref) {
    SnapshotRef snapshotRef =
        table.refs().get(ref == null ? SnapshotRef.MAIN_BRANCH : HiveUtils.getTableSnapshotRef(ref));
    return snapshotRef == null ? null : snapshotRef.snapshotId();
  }

  // a DV replaces every position delete of its data file
  private static Map<String, DeleteFileSet> rewritableDeletes(BatchScan scan) {
    Map<String, DeleteFileSet> rewritableDeletes = Maps.newHashMap();

    try (CloseableIterable<ScanTask> tasksIterable = scan.planFiles()) {
      tasksIterable.forEach(task -> {
        FileScanTask fileScanTask = task.asFileScanTask();

        fileScanTask.deletes().forEach(deleteFile -> {
          if (deleteFile.content() != FileContent.EQUALITY_DELETES) {
            rewritableDeletes
                .computeIfAbsent(fileScanTask.file().location(), ignored -> DeleteFileSet.create())
                .add(deleteFile);
          }
        });
      });
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to close table scan: %s", scan), e);
    }
    return rewritableDeletes;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, DeleteFileSet> read(Table table, String location) {
    LOG.info("Loading rewritable deletes from {}", location);
    try (ObjectInputStream in = new ObjectInputStream(
        new GZIPInputStream(table.io().newInputFile(location).newStream()))) {
      return (Map<String, DeleteFileSet>) in.readObject();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to read rewritable deletes from %s", location), e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(String.format("Failed to read rewritable deletes from %s", location), e);
    }
  }

  private static String location(Table table, Configuration conf) {
    return table.location() + "/temp/" + queryId(conf) + "-rewritable-deletes";
  }

  private static String queryId(Configuration conf) {
    return conf.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
  }

  // keeps the value the object cache logs short
  private record Cached(Map<String, DeleteFileSet> deletes) {
    @Override
    public String toString() {
      return "rewritable deletes of " + deletes.size() + " data files";
    }
  }
}
