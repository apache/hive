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

package org.apache.hadoop.hive.ql.ddl.table.storage.archive;

import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils.PartSpecInfo;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.ImmutableList;

import static org.apache.hadoop.hive.ql.ddl.table.storage.archive.AlterTableArchiveUtils.ARCHIVE_NAME;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;

/**
 * Operation process of truncating a table.
 */
public class AlterTableUnarchiveOperation extends DDLOperation<AlterTableUnarchiveDesc> {
  public AlterTableUnarchiveOperation(DDLOperationContext context, AlterTableUnarchiveDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException, URISyntaxException {
    Table table = context.getDb().getTable(desc.getTableName());
    if (table.getTableType() != TableType.MANAGED_TABLE) {
      throw new HiveException("UNARCHIVE can only be performed on managed tables");
    }

    Map<String, String> partitionSpec = desc.getPartitionSpec();
    if (partitionSpec == null) {
      throw new HiveException("UNARCHIVE is for partitions only");
    }

    PartSpecInfo partitionSpecInfo = PartSpecInfo.create(table, partitionSpec);
    List<Partition> partitions = context.getDb().getPartitions(table, partitionSpec);

    // The originalDir directory represents the original directory of the partitions' files. They now contain an
    // archived version of those files eg. hdfs:/warehouse/myTable/ds=1/
    Path originalDir = getOriginalDir(table, partitionSpecInfo, partitions);
    Path intermediateArchivedDir = AlterTableArchiveUtils.getInterMediateDir(originalDir, context.getConf(),
        ConfVars.METASTORE_INT_ARCHIVED);
    Path intermediateExtractedDir = AlterTableArchiveUtils.getInterMediateDir(originalDir, context.getConf(),
        ConfVars.METASTORE_INT_EXTRACTED);

    boolean recovery = isRecovery(intermediateArchivedDir, intermediateExtractedDir);

    for (Partition partition : partitions) {
      checkArchiveProperty(partitionSpec.size(), recovery, partition);
    }

    // assume the archive is in the original dir, check if it exists
    // The source directory is the directory containing all the files that should be in the partitions.
    // e.g. har:/warehouse/myTable/ds=1/myTable.har/ - Note the har:/ scheme
    Path archivePath = new Path(originalDir, ARCHIVE_NAME);
    URI archiveUri = archivePath.toUri();
    URI originalUri = ArchiveUtils.addSlash(originalDir.toUri());
    ArchiveUtils.HarPathHelper harHelper = new ArchiveUtils.HarPathHelper(context.getConf(), archiveUri, originalUri);
    URI sourceUri = harHelper.getHarUri(originalUri);
    Path sourceDir = new Path(sourceUri.getScheme(), sourceUri.getAuthority(), sourceUri.getPath());

    if (!HdfsUtils.pathExists(intermediateArchivedDir, context.getConf()) &&
        !HdfsUtils.pathExists(archivePath, context.getConf())) {
      throw new HiveException("Haven't found any archive where it should be");
    }

    Path tmpPath = context.getContext().getExternalTmpPath(originalDir);

    FileSystem fs = null;
    try {
      fs = tmpPath.getFileSystem(context.getConf());
    } catch (IOException e) {
      throw new HiveException(e);
    }

    if (!recovery) {
      extractArchiveToIntermediateDir(intermediateExtractedDir, sourceDir, tmpPath, fs);
    }

    // At this point, we know that the extracted files are in the intermediate extracted dir, or in the the original dir

    moveOriginalDirToIntermediateDir(originalDir, intermediateArchivedDir, fs);

    // If there is a failure from here to until when the metadata is changed, the partition will be empty or throw
    // errors on read. If the original location exists here, then it must be the extracted files because in the previous
    // step, we moved the previous original location (containing the archived files) to intermediateArchiveDir

    moveIntermediateExtractedDirToOriginalParent(originalDir, intermediateExtractedDir, fs);

    writeUnarchivationToMetastore(partitions);

    // If a failure happens here, the intermediate archive files won't be
    // deleted. The user will need to call unarchive again to clear those up.

    deleteIntermediateArchivedDir(table, intermediateArchivedDir);

    if (recovery) {
      context.getConsole().printInfo("Recovery after UNARCHIVE succeeded");
    }

    return 0;
  }

  private Path getOriginalDir(Table table, PartSpecInfo partitionSpecInfo, List<Partition> partitions)
      throws HiveException {
    Path originalDir = null;

    // when we have partial partitions specification we must assume partitions
    // lie in standard place - if they were in custom locations putting
    // them into one archive would involve mass amount of copying
    // in full partition specification case we allow custom locations
    // to keep backward compatibility
    if (partitions.isEmpty()) {
      throw new HiveException("No partition matches the specification");
    } else if (partitionSpecInfo.values.size() != table.getPartCols().size()) {
      // for partial specifications we need partitions to follow the scheme
      for (Partition partition : partitions) {
        if (AlterTableArchiveUtils.partitionInCustomLocation(table, partition)) {
          String message = String.format("UNARCHIVE cannot run for partition groups with custom locations like %s",
              partition.getLocation());
          throw new HiveException(message);
        }
      }
      originalDir = partitionSpecInfo.createPath(table);
    } else {
      Partition partition = partitions.get(0);
      if (ArchiveUtils.isArchived(partition)) {
        originalDir = new Path(AlterTableArchiveUtils.getOriginalLocation(partition));
      } else {
        originalDir = new Path(partition.getLocation());
      }
    }
    return originalDir;
  }

  private boolean isRecovery(Path intermediateArchivedDir, Path intermediateExtractedDir) throws HiveException {
    if (HdfsUtils.pathExists(intermediateArchivedDir, context.getConf()) ||
        HdfsUtils.pathExists(intermediateExtractedDir, context.getConf())) {
      context.getConsole().printInfo("Starting recovery after failed UNARCHIVE");
      return true;
    }
    return false;
  }

  private void checkArchiveProperty(int partitionSpecLevel, boolean recovery, Partition partition)
      throws HiveException {
    if (!ArchiveUtils.isArchived(partition) && !recovery) {
      throw new HiveException("Partition " + partition.getName() + " is not archived.");
    }
    int archiveLevel = ArchiveUtils.getArchivingLevel(partition);
    if (partitionSpecLevel > archiveLevel) {
      throw new HiveException("Partition " + partition.getName() + " is archived at level " + archiveLevel +
          ", and given partspec only has " + partitionSpecLevel + " specs.");
    }
  }

  private void extractArchiveToIntermediateDir(Path intermediateExtractedDir, Path sourceDir, Path tmpPath,
      FileSystem fs) throws HiveException {
    try {
      // Copy the files out of the archive into the temporary directory
      String copySource = sourceDir.toString();
      String copyDest = tmpPath.toString();
      List<String> args = ImmutableList.of("-cp", copySource, copyDest);

      context.getConsole().printInfo("Copying " + copySource + " to " + copyDest);
      FileSystem srcFs = FileSystem.get(sourceDir.toUri(), context.getConf());
      srcFs.initialize(sourceDir.toUri(), context.getConf());

      FsShell fss = new FsShell(context.getConf());
      int ret = 0;
      try {
        ret = ToolRunner.run(fss, args.toArray(new String[0]));
      } catch (Exception e) {
        throw new HiveException(e);
      }

      if (ret != 0) {
        throw new HiveException("Error while copying files from archive, return code=" + ret);
      } else {
        context.getConsole().printInfo("Successfully Copied " + copySource + " to " + copyDest);
      }

      context.getConsole().printInfo("Moving " + tmpPath + " to " + intermediateExtractedDir);
      if (fs.exists(intermediateExtractedDir)) {
        throw new HiveException("Invalid state: the intermediate extracted  directory already exists.");
      }
      fs.rename(tmpPath, intermediateExtractedDir);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private void moveOriginalDirToIntermediateDir(Path originalDir, Path intermediateArchivedDir, FileSystem fs)
      throws HiveException {
    if (!HdfsUtils.pathExists(intermediateArchivedDir, context.getConf())) {
      try {
        context.getConsole().printInfo("Moving " + originalDir + " to " + intermediateArchivedDir);
        fs.rename(originalDir, intermediateArchivedDir);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    } else {
      context.getConsole().printInfo(intermediateArchivedDir + " already exists. " +
          "Assuming it contains the archived version of the partition");
    }
  }

  private void moveIntermediateExtractedDirToOriginalParent(Path originalDir, Path intermediateExtractedDir,
      FileSystem fs) throws HiveException {
    if (!HdfsUtils.pathExists(originalDir, context.getConf())) {
      try {
        context.getConsole().printInfo("Moving " + intermediateExtractedDir + " to " + originalDir);
        fs.rename(intermediateExtractedDir, originalDir);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    } else {
      context.getConsole().printInfo(originalDir + " already exists. " +
          "Assuming it contains the extracted files in the partition");
    }
  }

  private void writeUnarchivationToMetastore(List<Partition> partitions) throws HiveException {
    for (Partition partition : partitions) {
      setUnArchived(partition);
      try {
        // TODO: use getDb().alterPartition with catalog name
        context.getDb().alterPartition(desc.getTableName(), partition, null, true);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
    }
  }

  /**
   * Sets the appropriate attributes in the supplied Partition object to mark it as not archived.
   * Note that the metastore is not touched - a separate call to alter_partition is needed.
   *
   * @param partition - the partition to modify
   */
  private void setUnArchived(Partition partition) {
    assert(ArchiveUtils.isArchived(partition));
    String parentDir = AlterTableArchiveUtils.getOriginalLocation(partition);
    AlterTableArchiveUtils.setIsArchived(partition, false, 0);
    AlterTableArchiveUtils.setOriginalLocation(partition, null);
    assert(parentDir != null);
    partition.setLocation(parentDir);
  }

  private void deleteIntermediateArchivedDir(Table table, Path intermediateArchivedDir) throws HiveException {
    if (HdfsUtils.pathExists(intermediateArchivedDir, context.getConf())) {
      boolean shouldEnableCm = ReplChangeManager.shouldEnableCm(context.getDb().getDatabase(table.getDbName()),
          table.getTTable());
      AlterTableArchiveUtils.deleteDir(intermediateArchivedDir, shouldEnableCm, context.getConf());
    }
  }
}
