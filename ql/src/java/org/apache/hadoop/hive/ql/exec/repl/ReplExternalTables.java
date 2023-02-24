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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.util.FileList;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.NEW_SNAPSHOT;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.OLD_SNAPSHOT;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.SnapshotCopyMode.DIFF_COPY;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.SnapshotCopyMode.FALLBACK_COPY;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.SnapshotCopyMode.INITIAL_COPY;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.firstSnapshot;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.secondSnapshot;

public class ReplExternalTables {
  private static final Logger LOG = LoggerFactory.getLogger(ReplExternalTables.class);
  private HiveConf hiveConf;
  private boolean includeExternalTables;
  private boolean dumpMetadataOnly;

  public ReplExternalTables(HiveConf conf) {
    this.hiveConf = conf;
    this.includeExternalTables = hiveConf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES);
    this.dumpMetadataOnly = hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY) ||
            hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE);
  }

  private boolean shouldWrite() {
    return !dumpMetadataOnly && includeExternalTables;
  }

  /**
   * this will dump a single line per external table. it can include additional lines for the same
   * table if the table is partitioned and the partition location is outside the table.
   * It returns list of all the external table locations.
   */
  void dataLocationDump(Table table, FileList fileList, HashMap<String, Boolean> singleCopyPaths,
      boolean isTableLevelReplication, HiveConf conf) throws IOException, HiveException {
    if (!shouldWrite()) {
      return;
    }
    if (!TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
      throw new IllegalArgumentException(
              "only External tables can be written via this writer, provided table is " + table
                      .getTableType());
    }
    Path fullyQualifiedDataLocation = PathBuilder.fullyQualifiedHDFSUri(table.getDataLocation(), FileSystem.get(hiveConf));
    if (isTableLevelReplication ||  !isPathWithinSubtree(table.getDataLocation(), singleCopyPaths)) {
      // Pass the snapshot related arguments as null, since snapshots won't be created for tables following this path
      // . The configured tables would be skipped here as they would be as part of singleCopyPaths, for which tasks
      // shall be created in the end
      dirLocationToCopy(table.getTableName(), fileList, fullyQualifiedDataLocation, conf, false, null,
          null, null, null, false);
    }
    if (table.isPartitioned()) {
      List<Partition> partitions;
      try {
        partitions = Hive.get(hiveConf).getPartitions(table);
      } catch (HiveException e) {
        if (e.getCause() instanceof NoSuchObjectException) {
          // If table is dropped when dump in progress, just skip partitions data location dump
          LOG.debug(e.getMessage());
          return;
        }
        throw e;
      }

      for (Partition partition : partitions) {
        boolean partitionLocOutsideTableLoc = !FileUtils.isPathWithinSubtree(
                partition.getDataLocation(), table.getDataLocation()
        );
        if (partitionLocOutsideTableLoc) {
          // check if the partition is outside the table location but inside any of the single copy task paths, in
          // that case we need not to create a task for this partition, that would get covered by one of the single
          // copy paths
          if (!isTableLevelReplication && isPathWithinSubtree(partition.getDataLocation(), singleCopyPaths)) {
            partitionLocOutsideTableLoc = false;
          }
        }
        if (partitionLocOutsideTableLoc) {
          fullyQualifiedDataLocation = PathBuilder
                  .fullyQualifiedHDFSUri(partition.getDataLocation(), FileSystem.get(hiveConf));
          // Passing null for snapshot related configs, since the creation of snapshots is explicitly turned off here.
          dirLocationToCopy(table.getTableName(), fileList, fullyQualifiedDataLocation, conf, false, null, null, null,
              null, false);
        }
      }
    }
  }

  /**
   * Creates copy task for the paths configured, irrespective of which table/partition belongs to it.
   * @param singlePathLocations paths to be copied. if the value is true then only the task is created.
   * @param fileList the tracking file which maintains the list of tasks.
   * @param conf Hive Configuration.
   * @throws Exception in case of any error.
   */
  void dumpNonTableLevelCopyPaths(HashMap<String, Boolean> singlePathLocations, FileList fileList, HiveConf conf,
      boolean isSnapshotEnabled, String snapshotPrefix, SnapshotUtils.ReplSnapshotCount replSnapshotCount,
      FileList snapPathFileList, ArrayList<String> prevSnaps, boolean isBootstrap) throws HiveException, IOException {
    for (Map.Entry<String, Boolean> location : singlePathLocations.entrySet()) {
      if (!StringUtils.isEmpty(location.getKey()) && location.getValue()) {
        Path fullyQualifiedDataLocation =
            PathBuilder.fullyQualifiedHDFSUri(new Path(location.getKey()), FileSystem.get(hiveConf));
        dirLocationToCopy("dbPath:" + fullyQualifiedDataLocation.getName(), fileList, fullyQualifiedDataLocation, conf,
            isSnapshotEnabled, snapshotPrefix, replSnapshotCount, snapPathFileList, prevSnaps, isBootstrap);
      }
    }
  }

  private boolean isPathWithinSubtree(Path path, HashMap<String, Boolean> parentPaths) {
    boolean response = false;
    for (Map.Entry<String, Boolean> parent : parentPaths.entrySet()) {
      if (!StringUtils.isEmpty(parent.getKey())) {
        response = FileUtils.isPathWithinSubtree(path, new Path(parent.getKey()));
      }
      if (response) {
        parent.setValue(true);
        break;
      }
    }
    return response;
  }

  private void dirLocationToCopy(String tableName, FileList fileList, Path sourcePath, HiveConf conf,
      boolean createSnapshot, String snapshotPrefix, SnapshotUtils.ReplSnapshotCount replSnapshotCount,
      FileList snapPathFileList, ArrayList<String> prevSnaps, boolean isBootstrap) throws HiveException, IOException {
    Path basePath = getExternalTableBaseDir(conf);
    Path targetPath = externalTableDataPath(conf, basePath, sourcePath);
    SnapshotUtils.SnapshotCopyMode copyMode =
        createSnapshotsAtSource(sourcePath, snapshotPrefix, createSnapshot, conf, replSnapshotCount, snapPathFileList,
            prevSnaps, isBootstrap);
    //Here, when src and target are HA clusters with same NS, then sourcePath would have the correct host
    //whereas the targetPath would have an host that refers to the target cluster. This is fine for
    //data-copy running during dump as the correct logical locations would be used. But if data-copy runs during
    //load, then the remote location needs to point to the src cluster from where the data would be copied and
    //the common original NS would suffice for targetPath.
    if (hiveConf.getBoolVar(HiveConf.ConfVars.REPL_HA_DATAPATH_REPLACE_REMOTE_NAMESERVICE) &&
            hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET)) {
      String remoteNS = hiveConf.get(HiveConf.ConfVars.REPL_HA_DATAPATH_REPLACE_REMOTE_NAMESERVICE_NAME.varname);
      if (StringUtils.isEmpty(remoteNS)) {
        throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE
                .format("Configuration 'hive.repl.ha.datapath.replace.remote.nameservice.name' is not valid "
                        + remoteNS == null ? "null" : remoteNS, ReplUtils.REPL_HIVE_SERVICE));
      }
      targetPath = new Path(Utils.replaceHost(targetPath.toString(), sourcePath.toUri().getHost()));
      sourcePath = new Path(Utils.replaceHost(sourcePath.toString(), remoteNS));
    }
    fileList.add(new DirCopyWork(tableName, sourcePath, targetPath, copyMode, snapshotPrefix).convertToString());
  }

  private SnapshotUtils.SnapshotCopyMode createSnapshotsAtSource(Path sourcePath, String snapshotPrefix,
      boolean isSnapshotEnabled, HiveConf conf, SnapshotUtils.ReplSnapshotCount replSnapshotCount, FileList snapPathFileList,
      ArrayList<String> prevSnaps, boolean isBootstrap) throws IOException {
    if (!isSnapshotEnabled) {
      LOG.info("Snapshot copy not enabled for path {} Will use normal distCp for copying data.", sourcePath);
      return FALLBACK_COPY;
    }
    DistributedFileSystem sourceDfs = SnapshotUtils.getDFS(sourcePath, conf);
    try {
      if(isBootstrap) {
        // Delete any pre existing snapshots.
        SnapshotUtils.deleteSnapshotIfExists(sourceDfs, sourcePath, firstSnapshot(snapshotPrefix), conf);
        SnapshotUtils.deleteSnapshotIfExists(sourceDfs, sourcePath, secondSnapshot(snapshotPrefix), conf);
        allowAndCreateInitialSnapshot(sourcePath, snapshotPrefix, conf, replSnapshotCount, snapPathFileList, sourceDfs);
        return INITIAL_COPY;
      }

      if (prevSnaps.contains(sourcePath.toString())) {
        // We already created a snapshot for this, just refresh the latest snapshot and leave.
        sourceDfs.deleteSnapshot(sourcePath, secondSnapshot(snapshotPrefix));
        replSnapshotCount.incrementNumDeleted();
        SnapshotUtils.createSnapshot(sourceDfs, sourcePath, secondSnapshot(snapshotPrefix), conf);
        replSnapshotCount.incrementNumCreated();
        snapPathFileList.add(sourcePath.toString());
        return SnapshotUtils
            .isSnapshotAvailable(sourceDfs, sourcePath, snapshotPrefix, OLD_SNAPSHOT, conf) ? DIFF_COPY : INITIAL_COPY;
      }
      // check if second snapshot exists.
      boolean isSecondSnapAvlb = SnapshotUtils.isSnapshotAvailable(sourceDfs, sourcePath, snapshotPrefix,
          OLD_SNAPSHOT, conf);
      if (isSecondSnapAvlb) {
        sourceDfs.deleteSnapshot(sourcePath, firstSnapshot(snapshotPrefix));
        replSnapshotCount.incrementNumDeleted();
        sourceDfs.renameSnapshot(sourcePath, secondSnapshot(snapshotPrefix), firstSnapshot(snapshotPrefix));
        SnapshotUtils.createSnapshot(sourceDfs, sourcePath, secondSnapshot(snapshotPrefix), conf);
        replSnapshotCount.incrementNumCreated();
        snapPathFileList.add(sourcePath.toString());
        return DIFF_COPY;
      } else {
        // Check if first snapshot is available
        boolean isFirstSnapshotAvailable =
            SnapshotUtils.isSnapshotAvailable(sourceDfs, sourcePath, snapshotPrefix, NEW_SNAPSHOT, conf);
        if (isFirstSnapshotAvailable) {
          sourceDfs.renameSnapshot(sourcePath, secondSnapshot(snapshotPrefix), firstSnapshot(snapshotPrefix));
          SnapshotUtils.createSnapshot(sourceDfs, sourcePath, secondSnapshot(snapshotPrefix), conf);
          replSnapshotCount.incrementNumCreated();
          snapPathFileList.add(sourcePath.toString());
          return DIFF_COPY;
        } else {
          allowAndCreateInitialSnapshot(sourcePath, snapshotPrefix, conf, replSnapshotCount, snapPathFileList,
              sourceDfs);
          return INITIAL_COPY;
        }
      }
    } catch (FileNotFoundException fnf) {
      // Source deleted is already handled and is not an abnormal scenario, log and return.
      LOG.debug("Can not enable snapshot for path: {}", sourcePath, fnf);
      return FALLBACK_COPY;
    }
  }

  private void allowAndCreateInitialSnapshot(Path sourcePath, String snapshotPrefix, HiveConf conf,
      SnapshotUtils.ReplSnapshotCount replSnapshotCount, FileList snapPathFileList, DistributedFileSystem sourceDfs)
      throws IOException {
    SnapshotUtils.allowSnapshot(sourceDfs, sourcePath, conf);
    SnapshotUtils.createSnapshot(sourceDfs, sourcePath, secondSnapshot(snapshotPrefix), conf);
    replSnapshotCount.incrementNumCreated();
    snapPathFileList.add(sourcePath.toString());
  }

  public static String externalTableLocation(HiveConf hiveConf, String location) throws SemanticException {
    Path basePath = getExternalTableBaseDir(hiveConf);
    Path currentPath = new Path(location);
    Path dataLocation = externalTableDataPath(hiveConf, basePath, currentPath);

    LOG.info("Incoming external table location: {} , new location: {}", location, dataLocation.toString());
    return dataLocation.toString();
  }

  public static Path getExternalTableBaseDir(HiveConf hiveConf) throws SemanticException {
    String baseDir = hiveConf.get(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname);
    URI baseDirUri  = StringUtils.isEmpty(baseDir) ? null : new Path(baseDir).toUri();
    if (baseDirUri == null || baseDirUri.getScheme() == null || baseDirUri.getAuthority() == null) {
      throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.format(
              String.format("Fully qualified path for 'hive.repl.replica.external.table.base.dir' is required %s",
                      baseDir == null ? "" : "- ('" + baseDir + "')"), ReplUtils.REPL_HIVE_SERVICE));
    }
    return new Path(baseDirUri);
  }

  public static Path externalTableDataPath(HiveConf hiveConf, Path basePath, Path sourcePath)
          throws SemanticException {
    String baseUriPath = basePath.toUri().getPath();
    String sourceUriPath = sourcePath.toUri().getPath();

    // "/" is input for base directory, then we should use exact same path as source or else append
    // source path under the base directory.
    String targetPathWithoutSchemeAndAuth
            = "/".equalsIgnoreCase(baseUriPath) ? sourceUriPath : (baseUriPath + sourceUriPath);
    Path dataPath;
    try {
      dataPath = PathBuilder.fullyQualifiedHDFSUri(
              new Path(targetPathWithoutSchemeAndAuth),
              basePath.getFileSystem(hiveConf)
      );
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.format(
        ErrorMsg.INVALID_PATH.getMsg(), ReplUtils.REPL_HIVE_SERVICE), e);
    }
    return dataPath;
  }
}
