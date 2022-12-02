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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.util.FileList;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.EximUtil.DataCopyPath;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FileOperations;
import org.apache.hadoop.hive.ql.plan.ExportWork.MmContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.toWriteEntity;

// TODO: this object is created once to call one method and then immediately destroyed.
//       So it's basically just a roundabout way to pass arguments to a static method. Simplify?
public class TableExport {
  private static final Logger logger = LoggerFactory.getLogger(TableExport.class);

  private TableSpec tableSpec;
  private final ReplicationSpec replicationSpec;
  private final Hive db;
  private final String distCpDoAsUser;
  private final HiveConf conf;
  private final Paths paths;
  private final MmContext mmCtx;

  public TableExport(Paths paths, TableSpec tableSpec, ReplicationSpec replicationSpec, Hive db,
      String distCpDoAsUser, HiveConf conf, MmContext mmCtx) {
    this.tableSpec = (tableSpec != null
        && tableSpec.tableHandle.isTemporary()
        && replicationSpec.isInReplicationScope())
        ? null
        : tableSpec;
    this.replicationSpec = replicationSpec;
    if (this.tableSpec != null && this.tableSpec.tableHandle!=null) {
      //If table is view or if should dump metadata only flag used by DAS is set to true
      //enable isMetadataOnly
      if (this.tableSpec.tableHandle.isView() || Utils.shouldDumpMetaDataOnly(conf)) {
        this.tableSpec.tableHandle.setStatsStateLikeNewTable();
        this.replicationSpec.setIsMetadataOnly(true);
      }
      //If table is view or if should dump metadata only for external table flag is set to true
      //enable isMetadataOnlyForExternalTable
      if (this.tableSpec.tableHandle.isView()
              || Utils.shouldDumpMetaDataOnlyForExternalTables(this.tableSpec.tableHandle, conf)) {
        this.tableSpec.tableHandle.setStatsStateLikeNewTable();
        this.replicationSpec.setMetadataOnlyForExternalTables(true);
      }
    }
    this.db = db;
    this.distCpDoAsUser = distCpDoAsUser;
    this.conf = conf;
    this.paths = paths;
    this.mmCtx = mmCtx;
  }

  @VisibleForTesting
  public TableExport() {
    this.replicationSpec = null;
    this.tableSpec = null;
    this.db = null;
    this.conf = null;
    this.distCpDoAsUser = "";
    this.paths = null;
    this.mmCtx = null;
  }

  /**
   * Write table or partition data one after another
   * @param isExportTask Indicates whether task is export or not
   * @param fileList List of files to be written
   * @param dataCopyAtLoad Indicates whether data need to be distcp during load only or not
   * @throws SemanticException
   */
  public void serialWrite(boolean isExportTask, FileList fileList, boolean dataCopyAtLoad) throws SemanticException {
    write(isExportTask, fileList, dataCopyAtLoad);
  }

  /**
   * Write table or partition data simultaneously using ExportService.
   * Create ExportJob to write data and submit it to the ExportService.
   * @param isExportTask Indicates whether task is export or not
   * @param fileList List of files to be written
   * @param dataCopyAtLoad Indicates whether data need to be distcp during load only or not.
   * @throws HiveException
   */
  public void parallelWrite(ExportService exportService, boolean isExportTask, FileList fileList, boolean dataCopyAtLoad) throws HiveException {
    assert (exportService != null && exportService.isExportServiceRunning());
    exportService.submit(() -> {
      if (tableSpec != null) {
        logger.debug("Starting parallel export of table {} ", tableSpec.getTableName());
      }
      try {
        write(isExportTask, fileList, dataCopyAtLoad);
        //As soon as write is over, close current thread MS client connection with Metastore server.
        Hive.closeCurrent();
      } catch (Exception e) {
        throw new RuntimeException(e.getCause().getMessage(), e.getCause());
      }
    }
    );
  }

  protected void write(boolean isExportTask, FileList fileList, boolean dataCopyAtLoad) throws SemanticException {
    if (tableSpec == null) {
      writeMetaData(null);
    } else if (shouldExport()) {
      PartitionIterable withPartitions = getPartitions();
      writeMetaData(withPartitions);
      if (!replicationSpec.isMetadataOnly()
              && !(replicationSpec.isRepl() && tableSpec.tableHandle.getTableType().equals(TableType.EXTERNAL_TABLE))) {
        writeData(withPartitions, isExportTask, fileList, dataCopyAtLoad);
      }
    } else if (isExportTask) {
      throw new SemanticException(ErrorMsg.INCOMPATIBLE_SCHEMA.getMsg());
    }
  }

  private PartitionIterable getPartitions() throws SemanticException {
    try {
      if (tableSpec != null && tableSpec.tableHandle != null && tableSpec.tableHandle.isPartitioned()) {
        if (tableSpec.specType == TableSpec.SpecType.TABLE_ONLY) {
          // TABLE-ONLY, fetch partitions if regular export, don't if metadata-only
          //For metadata only external tables, we still need the partition info
          if (replicationSpec.isMetadataOnly()) {
            return null;
          } else {
            // There are separate threads for exporting each table. getPartitions in each thread
            // has only one MS client embedded inside Hive object. When parallel thread
            // initiate connections to MetadataStore server, stream gets overwritten. So stream
            // becomes invalid for old client and its gets errors like socket closed or broken pipe
            // Hence, we create a local thread variable of Hive class and use it here while constructing
            // PartitionIterable object. This creates a local copy of MS client for each thread.

            return new PartitionIterable(Hive.get(conf), tableSpec.tableHandle, null, MetastoreConf.getIntVar(
                conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX), true);
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

  private void writeData(PartitionIterable partitions, boolean isExportTask, FileList fileList, boolean dataCopyAtLoad)
          throws SemanticException {
    try {
      if (tableSpec.tableHandle.isPartitioned()) {
        if (partitions == null) {
          throw new IllegalStateException("partitions cannot be null for partitionTable :"
              + tableSpec.getTableName().getTable());
        }
        new PartitionExport(paths, partitions, distCpDoAsUser, conf, mmCtx).write(replicationSpec, isExportTask,
                fileList, dataCopyAtLoad);
      } else {
        List<Path> dataPathList = Utils.getDataPathList(tableSpec.tableHandle.getDataLocation(),
                replicationSpec, conf);
        if (!(isExportTask || dataCopyAtLoad)) {
          fileList.add(new DataCopyPath(replicationSpec, tableSpec.tableHandle.getDataLocation(),
              paths.dataExportDir()).convertToString());
        }
        new FileOperations(dataPathList, paths.dataExportDir(), distCpDoAsUser, conf, mmCtx)
                .export(isExportTask, (dataCopyAtLoad));
      }
    } catch (Exception e) {
      throw new SemanticException(e.getMessage(), e);
    }
  }

  private boolean shouldExport() {
    return Utils.shouldReplicate(replicationSpec, tableSpec.tableHandle,
            false, null, null, conf);
  }

  /**
   * this class is responsible for giving various paths to be used during export along with root export
   * directory creation.
   */
  public static class Paths {
    private final String astRepresentationForErrorMsg;
    private final HiveConf conf;
    //metadataExportRootDir and dataExportRootDir variable access should not be done and use
    // metadataExportRootDir() and dataExportRootDir() instead.
    private final Path metadataExportRootDir;
    private final Path dataExportRootDir;
    private final FileSystem exportFileSystem;
    private boolean writeData, metadataExportRootDirCreated = false, dataExportRootDirCreated = false;

    public Paths(String astRepresentationForErrorMsg, Path dbMetadataRoot, Path dbDataRoot,
                 String tblName, HiveConf conf,
        boolean shouldWriteData) throws SemanticException {
      this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
      this.conf = conf;
      this.writeData = shouldWriteData;
      Path tableRootForMetadataDump = new Path(dbMetadataRoot, tblName);
      Path tableRootForDataDump = new Path(dbDataRoot, tblName);
      URI metadataExportRootDirUri = EximUtil.getValidatedURI(conf, tableRootForMetadataDump.toUri().toString());
      validateTargetDir(metadataExportRootDirUri);
      URI dataExportRootDirUri = EximUtil.getValidatedURI(conf, tableRootForDataDump.toUri().toString());
      validateTargetDataDir(dataExportRootDirUri);
      this.metadataExportRootDir = new Path(metadataExportRootDirUri);
      this.dataExportRootDir = new Path(dataExportRootDirUri);
      try {
        this.exportFileSystem = this.metadataExportRootDir.getFileSystem(conf);
      } catch (IOException e) {
        throw new SemanticException(e);
      }
    }

    public Paths(String astRepresentationForErrorMsg, String path, HiveConf conf,
        boolean shouldWriteData) throws SemanticException {
      this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
      this.conf = conf;
      this.metadataExportRootDir = new Path(EximUtil.getValidatedURI(conf, path));
      this.dataExportRootDir = new Path(new Path(EximUtil.getValidatedURI(conf, path)), EximUtil.DATA_PATH_NAME);
      this.writeData = shouldWriteData;
      try {
        this.exportFileSystem = metadataExportRootDir.getFileSystem(conf);
      } catch (IOException e) {
        throw new SemanticException(e);
      }
    }

    Path partitionMetadataExportDir(String partitionName) throws SemanticException {
      return exportDir(new Path(metadataExportRootDir(), partitionName));
    }

    Path partitionDataExportDir(String partitionName) throws SemanticException {
      return exportDir(new Path(dataExportRootDir(), partitionName));
    }

    /**
     * Access to the {@link #metadataExportRootDir} should only be done via this method
     * since the creation of the directory is delayed until we figure out if we want
     * to write something or not. This is specifically important to prevent empty non-native
     * directories being created in repl dump.
     */
    public Path metadataExportRootDir() throws SemanticException {
      if (!metadataExportRootDirCreated) {
        try {
          if (!exportFileSystem.exists(this.metadataExportRootDir) && writeData) {
            exportFileSystem.mkdirs(this.metadataExportRootDir);
          }
          metadataExportRootDirCreated = true;
        } catch (IOException e) {
          throw new SemanticException(e);
        }
      }
      return metadataExportRootDir;
    }

    /**
     * Access to the {@link #dataExportRootDir} should only be done via this method
     * since the creation of the directory is delayed until we figure out if we want
     * to write something or not. This is specifically important to prevent empty non-native
     * directories being created in repl dump.
     */
    public Path dataExportRootDir() throws SemanticException {
      if (!dataExportRootDirCreated) {
        try {
          if (!exportFileSystem.exists(this.dataExportRootDir) && writeData) {
            exportFileSystem.mkdirs(this.dataExportRootDir);
          }
          dataExportRootDirCreated = true;
        } catch (IOException e) {
          throw new SemanticException(e);
        }
      }
      return dataExportRootDir;
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

    private Path metaDataExportFile() throws SemanticException {
      return new Path(metadataExportRootDir(), EximUtil.METADATA_NAME);
    }

    /**
     * This is currently referring to the export path for the data within a non partitioned table.
     * Partition's data export directory is created within the export semantics of partition.
     */
    private Path dataExportDir() throws SemanticException {
      return exportDir(dataExportRootDir());
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

    /**
     * this level of validation might not be required as the root directory in which we dump will
     * be different for each run hence possibility of it having data is not there.
     */
    private void validateTargetDataDir(URI rootDirExportFile) throws SemanticException {
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
          }
        } catch (FileNotFoundException ignored) {
        }
      } catch (IOException e) {
        throw new SemanticException(astRepresentationForErrorMsg, e);
      }
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
      if (replicationSpec.isMetadataOnly() || replicationSpec.isMetadataOnlyForExternalTables()) {
        return authEntities;
      }
      PartitionIterable partitions = getPartitions();
      if (tableSpec != null) {
        if (tableSpec.tableHandle.isPartitioned()) {
          if (partitions == null) {
            throw new IllegalStateException("partitions cannot be null for partitionTable :"
                + tableSpec.getTableName().getTable());
          }
          for (Partition partition : partitions) {
            authEntities.inputs.add(new ReadEntity(partition));
          }
        } else {
          authEntities.inputs.add(new ReadEntity(tableSpec.tableHandle));
        }
      }
      authEntities.outputs.add(toWriteEntity(paths.metadataExportRootDir(), conf));
    } catch (Exception e) {
      throw new SemanticException(e);
    }
    return authEntities;
  }
}
