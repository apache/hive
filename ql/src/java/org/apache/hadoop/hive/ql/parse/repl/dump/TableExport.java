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
package org.apache.hadoop.hive.ql.parse.repl.dump;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.toWriteEntity;

public class TableExport {
  private static final Logger logger = LoggerFactory.getLogger(TableExport.class);

  private TableSpec tableSpec;
  private final ReplicationSpec replicationSpec;
  private final Hive db;
  private final String distCpDoAsUser;
  private final HiveConf conf;
  private final Paths paths;

  public TableExport(Paths paths, TableSpec tableSpec, ReplicationSpec replicationSpec, Hive db,
      String distCpDoAsUser, HiveConf conf) {
    this.tableSpec = (tableSpec != null
        && tableSpec.tableHandle.isTemporary()
        && replicationSpec.isInReplicationScope())
        ? null
        : tableSpec;
    this.replicationSpec = replicationSpec;
    if (conf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY) || (this.tableSpec != null
        && this.tableSpec.tableHandle.isView())) {
      this.replicationSpec.setIsMetadataOnly(true);
    }
    this.db = db;
    this.distCpDoAsUser = distCpDoAsUser;
    this.conf = conf;
    this.paths = paths;
  }

  public boolean write() throws SemanticException {
    if (tableSpec == null) {
      writeMetaData(null);
      return true;
    } else if (shouldExport()) {
      PartitionIterable withPartitions = getPartitions();
      writeMetaData(withPartitions);
      if (!replicationSpec.isMetadataOnly()) {
        writeData(withPartitions);
      }
      return true;
    }
    return false;
  }

  private PartitionIterable getPartitions() throws SemanticException {
    try {
      if (tableSpec != null && tableSpec.tableHandle != null && tableSpec.tableHandle.isPartitioned()) {
        if (tableSpec.specType == TableSpec.SpecType.TABLE_ONLY) {
          // TABLE-ONLY, fetch partitions if regular export, don't if metadata-only
          if (replicationSpec.isMetadataOnly()) {
            return null;
          } else {
            return new PartitionIterable(db, tableSpec.tableHandle, null, conf.getIntVar(
                HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX));
          }
        } else {
          // PARTITIONS specified - partitions inside tableSpec
          return new PartitionIterable(tableSpec.partitions);
        }
      } else {
        // Either tableHandle isn't partitioned => null, or repl-export after ts becomes null => null.
        // or this is a noop-replication export, so we can skip looking at ptns.
        return null;
      }
    } catch (HiveException e) {
      if (e.getCause() instanceof NoSuchObjectException) {
        // If table is dropped when dump in progress, just skip partitions dump
        return new PartitionIterable(new ArrayList<>());
      }
      throw new SemanticException("Error when identifying partitions", e);
    }
  }

  private void writeMetaData(PartitionIterable partitions) throws SemanticException {
    try {
      EximUtil.createExportDump(
          paths.exportFileSystem,
          paths.metaDataExportFile(),
          tableSpec == null ? null : tableSpec.tableHandle,
          partitions,
          replicationSpec,
          conf);
      logger.debug("_metadata file written into " + paths.metaDataExportFile().toString());
    } catch (Exception e) {
      // the path used above should not be used on a second try as each dump request is written to a unique location.
      // however if we want to keep the dump location clean we might want to delete the paths
      throw new SemanticException(
          ErrorMsg.IO_ERROR.getMsg("Exception while writing out the local file"), e);
    }
  }

  private void writeData(PartitionIterable partitions) throws SemanticException {
    try {
      if (tableSpec.tableHandle.isPartitioned()) {
        if (partitions == null) {
          throw new IllegalStateException("partitions cannot be null for partitionTable :"
              + tableSpec.tableName);
        }
        new PartitionExport(paths, partitions, distCpDoAsUser, conf).write(replicationSpec);
      } else {
        Path fromPath = tableSpec.tableHandle.getDataLocation();
        // this is the data copy
        new FileOperations(fromPath, paths.dataExportDir(), distCpDoAsUser, conf)
            .export(replicationSpec);
      }
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private boolean shouldExport() {
    // Note: this is a temporary setting that is needed because replication does not support
    //       ACID or MM tables at the moment. It will eventually be removed.
    if (conf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_INCLUDE_ACID_TABLES)
        && AcidUtils.isTransactionalTable(tableSpec.tableHandle)) {
      return true;
    }
    return Utils.shouldReplicate(replicationSpec, tableSpec.tableHandle, conf);
  }

  /**
   * this class is responsible for giving various paths to be used during export along with root export
   * directory creation.
   */
  public static class Paths {
    private final String astRepresentationForErrorMsg;
    private final HiveConf conf;
    private final Path exportRootDir;
    private final FileSystem exportFileSystem;
    private boolean writeData;

    public Paths(String astRepresentationForErrorMsg, Path dbRoot, String tblName, HiveConf conf,
        boolean shouldWriteData) throws SemanticException {
      this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
      this.conf = conf;
      this.writeData = shouldWriteData;
      Path tableRoot = new Path(dbRoot, tblName);
      URI exportRootDir = EximUtil.getValidatedURI(conf, tableRoot.toUri().toString());
      validateTargetDir(exportRootDir);
      this.exportRootDir = new Path(exportRootDir);
      try {
        this.exportFileSystem = this.exportRootDir.getFileSystem(conf);
        if (!exportFileSystem.exists(this.exportRootDir) && writeData) {
          exportFileSystem.mkdirs(this.exportRootDir);
        }
      } catch (IOException e) {
        throw new SemanticException(e);
      }
    }

    public Paths(String astRepresentationForErrorMsg, String path, HiveConf conf,
        boolean shouldWriteData) throws SemanticException {
      this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
      this.conf = conf;
      this.exportRootDir = new Path(EximUtil.getValidatedURI(conf, path));
      this.writeData = shouldWriteData;
      try {
        this.exportFileSystem = exportRootDir.getFileSystem(conf);
        if (!exportFileSystem.exists(this.exportRootDir) && writeData) {
          exportFileSystem.mkdirs(this.exportRootDir);
        }
      } catch (IOException e) {
        throw new SemanticException(e);
      }
    }

    Path partitionExportDir(String partitionName) throws SemanticException {
      return exportDir(new Path(exportRootDir, partitionName));
    }

    private Path exportDir(Path exportDir) throws SemanticException {
      try {
        if (!exportFileSystem.exists(exportDir)) {
          exportFileSystem.mkdirs(exportDir);
        }
        return exportDir;
      } catch (IOException e) {
        throw new SemanticException(
            "error while creating directory for partition at " + exportDir, e);
      }
    }

    private Path metaDataExportFile() {
      return new Path(exportRootDir, EximUtil.METADATA_NAME);
    }

    /**
     * This is currently referring to the export path for the data within a non partitioned table.
     * Partition's data export directory is created within the export semantics of partition.
     */
    private Path dataExportDir() throws SemanticException {
      return exportDir(new Path(getExportRootDir(), EximUtil.DATA_PATH_NAME));
    }

    /**
     * this level of validation might not be required as the root directory in which we dump will
     * be different for each run hence possibility of it having data is not there.
     */
    private void validateTargetDir(URI rootDirExportFile) throws SemanticException {
      try {
        FileSystem fs = FileSystem.get(rootDirExportFile, conf);
        Path toPath = new Path(rootDirExportFile.getScheme(), rootDirExportFile.getAuthority(),
            rootDirExportFile.getPath());
        try {
          FileStatus tgt = fs.getFileStatus(toPath);
          // target exists
          if (!tgt.isDirectory()) {
            throw new SemanticException(
                astRepresentationForErrorMsg + ": " + "Target is not a directory : "
                    + rootDirExportFile);
          } else {
            FileStatus[] files = fs.listStatus(toPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
            if (files != null && files.length != 0) {
              throw new SemanticException(
                  astRepresentationForErrorMsg + ": " + "Target is not an empty directory : "
                      + rootDirExportFile);
            }
          }
        } catch (FileNotFoundException ignored) {
        }
      } catch (IOException e) {
        throw new SemanticException(astRepresentationForErrorMsg, e);
      }
    }

    public Path getExportRootDir() {
      return exportRootDir;
    }
  }

  public static class AuthEntities {
    /**
     * This is  concurrent implementation as
     * @see org.apache.hadoop.hive.ql.parse.repl.dump.PartitionExport
     * uses multiple threads to flush out partitions.
     */
    public final Set<ReadEntity> inputs = Collections.newSetFromMap(new ConcurrentHashMap<>());
    public final Set<WriteEntity> outputs = new HashSet<>();
  }

  public AuthEntities getAuthEntities() throws SemanticException {
    AuthEntities authEntities = new AuthEntities();
    try {
      // Return if metadata-only
      if (replicationSpec.isMetadataOnly()) {
        return authEntities;
      }
      PartitionIterable partitions = getPartitions();
      if (tableSpec != null) {
        if (tableSpec.tableHandle.isPartitioned()) {
          if (partitions == null) {
            throw new IllegalStateException("partitions cannot be null for partitionTable :"
                + tableSpec.tableName);
          }
          for (Partition partition : partitions) {
            authEntities.inputs.add(new ReadEntity(partition));
          }
        } else {
          authEntities.inputs.add(new ReadEntity(tableSpec.tableHandle));
        }
      }
      authEntities.outputs.add(toWriteEntity(paths.getExportRootDir(), conf));
    } catch (Exception e) {
      throw new SemanticException(e);
    }
    return authEntities;
  }
}
