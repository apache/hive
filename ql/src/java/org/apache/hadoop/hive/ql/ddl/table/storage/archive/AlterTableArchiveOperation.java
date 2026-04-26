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
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils.PartSpecInfo;

import static org.apache.hadoop.hive.ql.ddl.table.storage.archive.AlterTableArchiveUtils.ARCHIVE_NAME;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.ImmutableList;

/**
 * Operation process of archiving a table.
 */
public class AlterTableArchiveOperation extends DDLOperation<AlterTableArchiveDesc> {
  public AlterTableArchiveOperation(DDLOperationContext context, AlterTableArchiveDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Table table = context.getDb().getTable(desc.getTableName());
    if (table.getTableType() != TableType.MANAGED_TABLE) {
      throw new HiveException("ARCHIVE can only be performed on managed tables");
    }

    Map<String, String> partitionSpec = desc.getPartitionSpec();
    PartSpecInfo partitionSpecInfo = PartSpecInfo.create(table, partitionSpec);
    List<Partition> partitions = context.getDb().getPartitions(table, partitionSpec);

    Path originalDir = getOriginalDir(table, partitionSpecInfo, partitions);
    Path intermediateArchivedDir = AlterTableArchiveUtils.getInterMediateDir(originalDir, context.getConf(),
        ConfVars.METASTORE_INT_ARCHIVED);
    Path intermediateOriginalDir = AlterTableArchiveUtils.getInterMediateDir(originalDir, context.getConf(),
        ConfVars.METASTORE_INT_ORIGINAL);

    context.getConsole().printInfo("intermediate.archived is " + intermediateArchivedDir.toString());
    context.getConsole().printInfo("intermediate.original is " + intermediateOriginalDir.toString());

    checkIfAlreadyArchived(partitionSpecInfo, partitions);
    boolean recovery = isRecovery(intermediateArchivedDir, intermediateOriginalDir);

    FileSystem fs = null;
    try {
      fs = originalDir.getFileSystem(context.getConf());
    } catch (IOException e) {
      throw new HiveException(e);
    }

    // The following steps seem roundabout, but they are meant to aid in recovery if a failure occurs and to keep a
    // consistent state in the FS

    // If the intermediate directory exists, we assume the dir is good to use as it's creation is atomic (move)
    if (!recovery) {
      Path tmpPath = createArchiveInTmpDir(table, partitionSpecInfo, originalDir);
      moveTmpDirToIntermediateDir(intermediateArchivedDir, fs, tmpPath);
    } else {
      if (HdfsUtils.pathExists(intermediateArchivedDir, context.getConf())) {
        context.getConsole().printInfo("Intermediate archive directory " + intermediateArchivedDir +
            " already exists. Assuming it contains an archived version of the partition");
      }
    }

    moveOriginalDirToIntermediateDir(originalDir, intermediateOriginalDir, fs);

    // If there's a failure from here to when the metadata is updated, there will be no data in the partition, or an
    // error while trying to read the partition (if the archive files have been moved to the original partition
    // directory.) But re-running the archive command will allow recovery

    moveIntermediateArchivedDirToOriginalParent(originalDir, intermediateArchivedDir, fs);

    writeArchivationToMetastore(partitionSpecInfo, partitions, originalDir);

    // If a failure occurs here, the directory containing the original files will not be deleted. The user will run
    // ARCHIVE again to clear this up. The presence of these files are used to indicate whether the original partition
    // directory contains archived or unarchived files.

    deleteIntermediateOriginalDir(table, intermediateOriginalDir);

    if (recovery) {
      context.getConsole().printInfo("Recovery after ARCHIVE succeeded");
    }

    return 0;
  }

  private Path getOriginalDir(Table table, PartSpecInfo partitionSpecInfo, List<Partition> partitions)
      throws HiveException {
    // when we have partial partitions specification we must assume partitions lie in standard place -
    // if they were in custom locations putting them into one archive would involve mass amount of copying
    // in full partition specification case we allow custom locations to keep backward compatibility
    if (partitions.isEmpty()) {
      throw new HiveException("No partition matches the specification");
    } else if (partitionSpecInfo.values.size() != table.getPartCols().size()) {
      // for partial specifications we need partitions to follow the scheme
      for (Partition partition : partitions) {
        if (AlterTableArchiveUtils.partitionInCustomLocation(table, partition)) {
          throw new HiveException(String.format("ARCHIVE cannot run for partition groups with custom locations like %s",
              partition.getLocation()));
        }
      }
      return partitionSpecInfo.createPath(table);
    } else {
      Partition p = partitions.get(0);
      // partition can be archived if during recovery
      if (ArchiveUtils.isArchived(p)) {
        return new Path(AlterTableArchiveUtils.getOriginalLocation(p));
      } else {
        return p.getDataLocation();
      }
    }
  }

  private void checkIfAlreadyArchived(PartSpecInfo partitionSpecInfo, List<Partition> partitions) throws HiveException {
    // we checked if partitions matching specification are marked as archived in the metadata; if they are then
    // throw an exception
    for (Partition partition : partitions) {
      if (ArchiveUtils.isArchived(partition)) {
        if (ArchiveUtils.getArchivingLevel(partition) != partitionSpecInfo.values.size()) {
          String name = ArchiveUtils.getPartialName(partition, ArchiveUtils.getArchivingLevel(partition));
          throw new HiveException(String.format("Conflict with existing archive %s", name));
        } else {
          throw new HiveException("Partition(s) already archived");
        }
      }
    }
  }

  private boolean isRecovery(Path intermediateArchivedDir, Path intermediateOriginalDir) throws HiveException {
    if (HdfsUtils.pathExists(intermediateArchivedDir, context.getConf()) ||
        HdfsUtils.pathExists(intermediateOriginalDir, context.getConf())) {
      context.getConsole().printInfo("Starting recovery after failed ARCHIVE");
      return true;
    }
    return false;
  }

  private Path createArchiveInTmpDir(Table table, PartSpecInfo partitionSpecInfo, Path originalDir)
      throws HiveException {
    // First create the archive in a tmp dir so that if the job fails, the bad files don't pollute the filesystem
    Path tmpPath = new Path(context.getContext().getExternalTmpPath(originalDir), "partlevel");

    // Create the Hadoop archive
    context.getConsole().printInfo("Creating " + ARCHIVE_NAME + " for " + originalDir.toString() + " in " + tmpPath);
    context.getConsole().printInfo("Please wait... (this may take a while)");
    try {
      int maxJobNameLength = context.getConf().getIntVar(HiveConf.ConfVars.HIVE_JOBNAME_LENGTH);
      String jobName = String.format("Archiving %s@%s", table.getTableName(), partitionSpecInfo.getName());
      jobName = Utilities.abbreviate(jobName, maxJobNameLength - 6);
      context.getConf().set(MRJobConfig.JOB_NAME, jobName);

      HadoopArchives har = new HadoopArchives(context.getConf());
      List<String> args = ImmutableList.of("-archiveName", ARCHIVE_NAME, "-p", originalDir.toString(),
          tmpPath.toString());

      int ret = ToolRunner.run(har, args.toArray(new String[0]));
      if (ret != 0) {
        throw new HiveException("Error while creating HAR");
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }

    return tmpPath;
  }

  /**
   * Move from the tmp dir to an intermediate directory, in the same level as the partition directory.
   * e.g. .../hr=12-intermediate-archived
   */
  private void moveTmpDirToIntermediateDir(Path intermediateArchivedDir, FileSystem fs, Path tmpPath)
      throws HiveException {
    try {
      context.getConsole().printInfo("Moving " + tmpPath + " to " + intermediateArchivedDir);
      if (HdfsUtils.pathExists(intermediateArchivedDir, context.getConf())) {
        throw new HiveException("The intermediate archive directory already exists.");
      }
      fs.rename(tmpPath, intermediateArchivedDir);
    } catch (IOException e) {
      throw new HiveException("Error while moving tmp directory");
    }
  }

  /**
   * Move the original parent directory to the intermediate original directory if the move hasn't been made already.
   */
  private void moveOriginalDirToIntermediateDir(Path originalDir, Path intermediateOriginalDir, FileSystem fs)
      throws HiveException {
    // If we get to here, we know that we've archived the partition files, but they may be in the original partition
    // location, or in the intermediate original dir.
    if (!HdfsUtils.pathExists(intermediateOriginalDir, context.getConf())) {
      context.getConsole().printInfo("Moving " + originalDir + " to " + intermediateOriginalDir);
      moveDir(fs, originalDir, intermediateOriginalDir);
    } else {
      context.getConsole().printInfo(intermediateOriginalDir + " already exists. " +
          "Assuming it contains the original files in the partition");
    }
  }

  /**
   * Move the intermediate archived directory to the original parent directory.
   */
  private void moveIntermediateArchivedDirToOriginalParent(Path originalDir, Path intermediateArchivedDir,
      FileSystem fs) throws HiveException {
    if (!HdfsUtils.pathExists(originalDir, context.getConf())) {
      context.getConsole().printInfo("Moving " + intermediateArchivedDir + " to " + originalDir);
      moveDir(fs, intermediateArchivedDir, originalDir);
    } else {
      context.getConsole().printInfo(originalDir + " already exists. Assuming it contains the archived version of "
          + "the partition");
    }
  }

  private void moveDir(FileSystem fs, Path from, Path to) throws HiveException {
    try {
      if (!fs.rename(from, to)) {
        throw new HiveException("Moving " + from + " to " + to + " failed!");
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Record the previous changes in the metastore.
   */
  private void writeArchivationToMetastore(PartSpecInfo partitionSpecInfo, List<Partition> partitions, Path originalDir)
      throws HiveException {
    try {
      URI archiveUri = new Path(originalDir, ARCHIVE_NAME).toUri();
      URI originalUri = ArchiveUtils.addSlash(originalDir.toUri());
      ArchiveUtils.HarPathHelper harHelper = new ArchiveUtils.HarPathHelper(context.getConf(), archiveUri, originalUri);

      for (Partition partition : partitions) {
        URI originalPartitionUri = ArchiveUtils.addSlash(partition.getDataLocation().toUri());
        URI harPartitionDir = harHelper.getHarUri(originalPartitionUri);
        StringBuilder authority = new StringBuilder();
        if (harPartitionDir.getUserInfo() != null) {
          authority.append(harPartitionDir.getUserInfo()).append("@");
        }
        authority.append(harPartitionDir.getHost());
        if (harPartitionDir.getPort() != -1) {
          authority.append(":").append(harPartitionDir.getPort());
        }

        // make in Path to ensure no slash at the end
        Path harPath = new Path(harPartitionDir.getScheme(), authority.toString(), harPartitionDir.getPath());
        setArchived(partition, harPath, partitionSpecInfo.values.size());
        // TODO: catalog
        context.getDb().alterPartition(desc.getTableName(), partition, null, true);
      }
    } catch (Exception e) {
      throw new HiveException("Unable to change the partition info for HAR", e);
    }
  }

  /**
   * Sets the appropriate attributes in the supplied Partition object to mark
   * it as archived. Note that the metastore is not touched - a separate
   * call to alter_partition is needed.
   *
   * @param p - the partition object to modify
   * @param harPath - new location of partition (har schema URI)
   */
  private void setArchived(Partition p, Path harPath, int level) {
    assert(!ArchiveUtils.isArchived(p));
    AlterTableArchiveUtils.setIsArchived(p, true, level);
    AlterTableArchiveUtils.setOriginalLocation(p, p.getLocation());
    p.setLocation(harPath.toString());
  }

  private void deleteIntermediateOriginalDir(Table table, Path intermediateOriginalDir) throws HiveException {
    if (HdfsUtils.pathExists(intermediateOriginalDir, context.getConf())) {
      boolean shouldEnableCm = ReplChangeManager.shouldEnableCm(context.getDb().getDatabase(table.getDbName()),
          table.getTTable());
      AlterTableArchiveUtils.deleteDir(intermediateOriginalDir, shouldEnableCm, context.getConf());
    }
  }
}
