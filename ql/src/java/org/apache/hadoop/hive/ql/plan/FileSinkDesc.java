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

package org.apache.hadoop.hive.ql.plan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.signature.Signature;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;

import static org.apache.hadoop.hive.ql.io.AcidUtils.COMPACTOR_TABLE_PROPERTY;

/**
 * FileSinkDesc.
 *
 */
@Explain(displayName = "File Output Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class FileSinkDesc extends AbstractOperatorDesc implements IStatsGatherDesc {
  private static final long serialVersionUID = 1L;

  public enum DPSortState {
    NONE, PARTITION_SORTED, PARTITION_BUCKET_SORTED
  }

  private DPSortState dpSortState;
  private Path dirName;
  // normally statsKeyPref will be the same as dirName, but the latter
  // could be changed in local execution optimization
  private String statsKeyPref;
  private TableDesc tableInfo;
  private boolean compressed;
  private int destTableId;
  private String compressCodec;
  private String compressType;
  private boolean multiFileSpray;
  private boolean temporary;
  private boolean materialization;
  // Whether the files output by this FileSink can be merged, e.g. if they are to be put into a
  // bucketed or sorted table/partition they cannot be merged.
  private boolean canBeMerged;
  private int     totalFiles;
  private List<ExprNodeDesc> partitionCols;
  private int     numFiles;
  private DynamicPartitionCtx dpCtx;
  private String staticSpec; // static partition spec ends with a '/'
  private boolean gatherStats;
  private String moveTaskId;

  // Consider a query like:
  // insert overwrite table T3 select ... from T1 join T2 on T1.key = T2.key;
  // where T1, T2 and T3 are sorted and bucketed by key into the same number of buckets,
  // We dont need a reducer to enforce bucketing and sorting for T3.
  // The field below captures the fact that the reducer introduced to enforce sorting/
  // bucketing of T3 has been removed.
  // In this case, a sort-merge join is needed, and so the sort-merge join between T1 and T2
  // cannot be performed as a map-only job
  private transient boolean removedReduceSinkBucketSort;

  // This file descriptor is linked to other file descriptors.
  // One use case is that, a union->select (star)->file sink, is broken down.
  // For eg: consider a query like:
  // select * from (subq1 union all subq2)x;
  // where subq1 or subq2 involves a map-reduce job.
  // It is broken into two independent queries involving subq1 and subq2 directly, and
  // the sub-queries write to sub-directories of a common directory. So, the file sink
  // descriptors for subq1 and subq2 are linked.
  private boolean linkedFileSink = false;
  private transient List<FileSinkDesc> linkedFileSinkDesc;

  private boolean statsReliable;
  private ListBucketingCtx lbCtx;
  private String statsTmpDir;

  // Record what type of write this is.  Default is non-ACID (ie old style).
  private AcidUtils.Operation writeType = AcidUtils.Operation.NOT_ACID;
  private Context.Operation writeOperation = Context.Operation.OTHER;
  private long tableWriteId = 0;  // table write id for this operation
  private int statementId = -1;
  private int maxStmtId = -1;

  private transient Table table;
  private Path destPath;
  private boolean isHiveServerQuery;
  private Long mmWriteId;
  private boolean isMerge;
  private boolean isMmCtas;

  private Set<FileStatus> filesToFetch = null;

  // contains the partition values for each dynamic Partition written by this FileSink
  // and the committed files for each partition
  private Map<String, List<Path>> dynPartitionValues = new HashMap<>();

  /**
   * Whether is a HiveServer query, and the destination table is
   * indeed written using a row batching SerDe
   */
  private boolean isUsingBatchingSerDe = false;

  private boolean isInsertOverwrite = false;

  private boolean isDirectInsert = false;

  private AcidUtils.Operation acidOperation = null;
  private boolean deleteOfSplitUpdate;

  private boolean isQuery = false;

  private boolean isCTASorCM = false;

  public FileSinkDesc() {
  }

  /**
   * @param destPath - the final destination for data
   */
  public FileSinkDesc(final Path dirName, final TableDesc tableInfo, final boolean compressed, final int destTableId,
      final boolean multiFileSpray, final boolean canBeMerged, final int numFiles, final int totalFiles,
      final List<ExprNodeDesc> partitionCols, final DynamicPartitionCtx dpCtx, Path destPath, Long mmWriteId,
      boolean isMmCtas, boolean isInsertOverwrite, boolean isQuery, boolean isCTASorCM, boolean isDirectInsert,
      AcidUtils.Operation acidOperation, boolean deleteOfSplitUpdate) {
    this.dirName = dirName;
    setTableInfo(tableInfo);
    this.compressed = compressed;
    this.destTableId = destTableId;
    this.multiFileSpray = multiFileSpray;
    this.canBeMerged = canBeMerged;
    this.numFiles = numFiles;
    this.totalFiles = totalFiles;
    this.partitionCols = partitionCols;
    this.dpCtx = dpCtx;
    this.dpSortState = DPSortState.NONE;
    this.destPath = destPath;
    this.mmWriteId = mmWriteId;
    this.isMmCtas = isMmCtas;
    this.isInsertOverwrite = isInsertOverwrite;
    this.isQuery = isQuery;
    this.isCTASorCM = isCTASorCM;
    this.isDirectInsert = isDirectInsert;
    this.acidOperation = acidOperation;
    this.deleteOfSplitUpdate = deleteOfSplitUpdate;
  }

  public FileSinkDesc(final Path dirName, final TableDesc tableInfo,
      final boolean compressed) {

    this.dirName = dirName;
    setTableInfo(tableInfo);
    this.compressed = compressed;
    destTableId = 0;
    this.multiFileSpray = false;
    this.canBeMerged = false;
    this.numFiles = 1;
    this.totalFiles = 1;
    this.partitionCols = null;
    this.dpSortState = DPSortState.NONE;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    FileSinkDesc ret = new FileSinkDesc(dirName, tableInfo, compressed, destTableId, multiFileSpray, canBeMerged,
        numFiles, totalFiles, partitionCols, dpCtx, destPath, mmWriteId, isMmCtas, isInsertOverwrite, isQuery,
        isCTASorCM, isDirectInsert, acidOperation, deleteOfSplitUpdate);
    ret.setCompressCodec(compressCodec);
    ret.setCompressType(compressType);
    ret.setGatherStats(gatherStats);
    ret.setStaticSpec(staticSpec);
    ret.setStatsAggPrefix(statsKeyPref);
    ret.setLinkedFileSink(linkedFileSink);
    ret.setLinkedFileSinkDesc(linkedFileSinkDesc);
    ret.setStatsReliable(statsReliable);
    ret.setDpSortState(dpSortState);
    ret.setWriteType(writeType);
    ret.setTableWriteId(tableWriteId);
    ret.setStatementId(statementId);
    ret.setStatsTmpDir(statsTmpDir);
    ret.setIsMerge(isMerge);
    ret.setFilesToFetch(filesToFetch);
    ret.setIsQuery(isQuery);
    ret.setIsCTASorCM(isCTASorCM);
    ret.setIsDirectInsert(isDirectInsert);
    ret.setAcidOperation(acidOperation);
    ret.setMoveTaskId(moveTaskId);
    return ret;
  }

  public void setFilesToFetch(Set<FileStatus> filesToFetch) {
    this.filesToFetch = filesToFetch;
  }

  public void setIsCTASorCM(boolean isCTASorCM) {
    this.isCTASorCM = isCTASorCM;
  }

  public void setIsQuery(boolean isQuery) {
    this.isQuery = isQuery;
  }

  public boolean getIsQuery() {
    return this.isQuery;
  }

  public Set<FileStatus> getFilesToFetch() {
    return filesToFetch;
  }

  public boolean isHiveServerQuery() {
    return this.isHiveServerQuery;
  }

  public void setHiveServerQuery(boolean isHiveServerQuery) {
    this.isHiveServerQuery = isHiveServerQuery;
  }

  public boolean isUsingBatchingSerDe() {
    return this.isUsingBatchingSerDe;
  }

  public void setIsUsingBatchingSerDe(boolean isUsingBatchingSerDe) {
    this.isUsingBatchingSerDe = isUsingBatchingSerDe;
  }

  public void setIsDirectInsert(boolean isDirectInsert) {
    this.isDirectInsert = isDirectInsert;
  }

  public boolean isDirectInsert() {
    return this.isDirectInsert;
  }

  public void setAcidOperation(AcidUtils.Operation acidOperation) {
    this.acidOperation = acidOperation;
  }

  public AcidUtils.Operation getAcidOperation() {
   return acidOperation;
  }

  public boolean isDeleteOfSplitUpdate() {
    return deleteOfSplitUpdate;
  }

  @Explain(displayName = "directory", explainLevels = { Level.EXTENDED })
  public Path getDirName() {
    return dirName;
  }

  @Signature
  public String getDirNameString() {
    return dirName.toString();
  }

  public void setDirName(final Path dirName) {
    this.dirName = dirName;
  }

  public Path getFinalDirName() {
    return linkedFileSink ? dirName.getParent() : dirName;
  }

  /** getFinalDirName that takes into account MM, but not DP, LB or buckets. */
  public Path getMergeInputDirName() {
    Path root = getFinalDirName();
    if (isMmTable()) {
      return new Path(root, AcidUtils.deltaSubdir(tableWriteId, tableWriteId, statementId));
    } else {
      return root;
    }
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public TableDesc getTableInfo() {
    return tableInfo;
  }

  public void setTableInfo(final TableDesc tableInfo) {
    this.tableInfo = tableInfo;
    bucketingVersion = tableInfo.getBucketingVersion();
  }

  @Explain(displayName = "compressed")
  @Signature
  public boolean getCompressed() {
    return compressed;
  }

  public void setCompressed(boolean compressed) {
    this.compressed = compressed;
  }

  @Explain(displayName = "GlobalTableId", explainLevels = { Level.EXTENDED })
  @Signature

  public int getDestTableId() {
    return destTableId;
  }

  public void setDestTableId(int destTableId) {
    this.destTableId = destTableId;
  }

  public String getCompressCodec() {
    return compressCodec;
  }

  public void setCompressCodec(String intermediateCompressorCodec) {
    compressCodec = intermediateCompressorCodec;
  }

  public String getCompressType() {
    return compressType;
  }

  public void setCompressType(String intermediateCompressType) {
    compressType = intermediateCompressType;
  }

  /**
   * @return the multiFileSpray
   */
  @Explain(displayName = "MultiFileSpray", explainLevels = { Level.EXTENDED })
  @Signature

  public boolean isMultiFileSpray() {
    return multiFileSpray;
  }

  /**
   * @param multiFileSpray the multiFileSpray to set
   */
  public void setMultiFileSpray(boolean multiFileSpray) {
    this.multiFileSpray = multiFileSpray;
  }

  /**
   * @return destination is temporary
   */
  public boolean isTemporary() {
    return temporary;
  }

  public void setTemporary(boolean temporary) {
    this.temporary = temporary;
  }

  public boolean isMmTable() {
    if (getTable() != null) {
      return AcidUtils.isInsertOnlyTable(table.getParameters());
    } else { // Dynamic Partition Insert case
      return AcidUtils.isInsertOnlyTable(getTableInfo().getProperties());
    }
  }
  public boolean isFullAcidTable() {
    if(getTable() != null) {
      return AcidUtils.isFullAcidTable(table);
    } else {
      return AcidUtils.isTablePropertyTransactional(getTableInfo().getProperties()) &&
          !AcidUtils.isInsertOnlyTable(getTableInfo().getProperties());
    }
  }

  /**
   * @return true, if the table is used during compaction
   */
  public boolean isCompactionTable() {
    return getTable() != null ? AcidUtils.isCompactionTable(table.getParameters())
        : AcidUtils.isCompactionTable(getTableInfo().getProperties());
  }

  /**
   * @return true if the compaction type is 'REBALANCE', false otherwise.
   */
  public boolean isRebalanceRequested() {
    String compactionType = getTable() != null
        ? table.getParameters().get(COMPACTOR_TABLE_PROPERTY)
        : getTableInfo().getProperties().getProperty(COMPACTOR_TABLE_PROPERTY);
    return StringUtils.isNotBlank(compactionType) &&
        CompactionType.valueOf(compactionType).equals(CompactionType.REBALANCE);
  }

  public boolean isMaterialization() {
    return materialization;
  }

  public void setMaterialization(boolean materialization) {
    this.materialization = materialization;
  }


  public boolean canBeMerged() {
    return canBeMerged;
  }

  public void setCanBeMerged(boolean canBeMerged) {
    this.canBeMerged = canBeMerged;
  }

  /**
   * @return the totalFiles
   */
  @Explain(displayName = "TotalFiles", explainLevels = { Level.EXTENDED })
  @Signature

  public int getTotalFiles() {
    return totalFiles;
  }

  /**
   * @param totalFiles the totalFiles to set
   */
  public void setTotalFiles(int totalFiles) {
    this.totalFiles = totalFiles;
  }

  /**
   * @return the partitionCols
   */
  public List<ExprNodeDesc> getPartitionCols() {
    return partitionCols;
  }

  /**
   * @param partitionCols the partitionCols to set
   */
  public void setPartitionCols(List<ExprNodeDesc> partitionCols) {
    this.partitionCols = partitionCols;
  }

  /**
   * @return the numFiles
   */
  @Explain(displayName = "NumFilesPerFileSink", explainLevels = { Level.EXTENDED })
  @Signature

  public int getNumFiles() {
    return numFiles;
  }

  /**
   * @param numFiles the numFiles to set
   */
  public void setNumFiles(int numFiles) {
    this.numFiles = numFiles;
  }

  public void setDynPartCtx(DynamicPartitionCtx dpc) {
    this.dpCtx = dpc;
  }

  public DynamicPartitionCtx getDynPartCtx() {
    return this.dpCtx;
  }

  public void setStaticSpec(String staticSpec) {
    this.staticSpec = staticSpec;
  }

  @Explain(displayName = "Static Partition Specification", explainLevels = { Level.EXTENDED })
  @Signature
  public String getStaticSpec() {
    return staticSpec;
  }

  public void setGatherStats(boolean gatherStats) {
    this.gatherStats = gatherStats;
  }

  @Override
  @Explain(displayName = "GatherStats", explainLevels = { Level.EXTENDED })
  @Signature

  public boolean isGatherStats() {
    return gatherStats;
  }

  /**
   * Construct the key prefix used as (intermediate) statistics publishing
   * and aggregation. During stats publishing phase, this key prefix will be
   * appended with the optional dynamic partition spec and the task ID. The
   * whole key uniquely identifies the output of a task for this job. In the
   * stats aggregation phase, all rows with the same prefix plus dynamic partition
   * specs (obtained at run-time after MR job finishes) will be serving as the
   * prefix: all rows with the same prefix (output of all tasks for this job)
   * will be aggregated.
   * @return key prefix used for stats publishing and aggregation.
   */
  @Override
  @Explain(displayName = "Stats Publishing Key Prefix", explainLevels = { Level.EXTENDED })
  // FIXME: including this in the signature will almost certainly differ even if the operator is doing the same
  // there might be conflicting usages of logicalCompare?
  @Signature
  public String getStatsAggPrefix() {
    // dirName uniquely identifies destination directory of a FileSinkOperator.
    // If more than one FileSinkOperator write to the same partition, this dirName
    // should be different.
    return statsKeyPref;
  }

  /**
   * Set the stats aggregation key. If the input string is not terminated by Path.SEPARATOR
   * aggregation key will add one to make it as a directory name.
   * @param k input directory name.
   */
  public void setStatsAggPrefix(String k) {
    if (k.endsWith(Path.SEPARATOR)) {
      statsKeyPref = k;
    } else {
      statsKeyPref = k + Path.SEPARATOR;
    }
  }

  public boolean isLinkedFileSink() {
    return linkedFileSink;
  }

  public void setLinkedFileSink(boolean linkedFileSink) {
    this.linkedFileSink = linkedFileSink;
  }

  public Path getParentDir() {
    return dirName.getParent();
  }

  public boolean isStatsReliable() {
    return statsReliable;
  }

  public void setStatsReliable(boolean statsReliable) {
    this.statsReliable = statsReliable;
  }

  /**
   * @return the lbCtx
   */
  public ListBucketingCtx getLbCtx() {
    return lbCtx;
  }

  /**
   * @param lbCtx the lbCtx to set
   */
  public void setLbCtx(ListBucketingCtx lbCtx) {
    this.lbCtx = lbCtx;
  }

  public List<FileSinkDesc> getLinkedFileSinkDesc() {
    return linkedFileSinkDesc;
  }

  public void setLinkedFileSinkDesc(List<FileSinkDesc> linkedFileSinkDesc) {
    this.linkedFileSinkDesc = linkedFileSinkDesc;
  }

  public boolean isRemovedReduceSinkBucketSort() {
    return removedReduceSinkBucketSort;
  }

  public void setRemovedReduceSinkBucketSort(boolean removedReduceSinkBucketSort) {
    this.removedReduceSinkBucketSort = removedReduceSinkBucketSort;
  }

  public DPSortState getDpSortState() {
    return dpSortState;
  }
  @Explain(displayName = "Dp Sort State")
  public String getDpSortStateString() {
    return getDpSortState() == DPSortState.NONE ? null : getDpSortState().toString();
  }
  public void setDpSortState(DPSortState dpSortState) {
    this.dpSortState = dpSortState;
  }

  public void setWriteType(AcidUtils.Operation type) {
    writeType = type;
  }

  public AcidUtils.Operation getWriteType() {
    return writeType;
  }

  public void setWriteOperation(Context.Operation writeOperation) {
    this.writeOperation = writeOperation;
  }

  public Context.Operation getWriteOperation() {
    return writeOperation;
  }

  @Explain(displayName = "Write Type")
  public String getWriteTypeString() {
    return getWriteType() == AcidUtils.Operation.NOT_ACID ? null : getWriteType().toString();
  }
  public void setTableWriteId(long id) {
    tableWriteId = id;
  }
  public long getTableWriteId() {
    return tableWriteId;
  }

  public void setStatementId(int id) {
    statementId = id;
  }
  /**
   * See {@link org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options#statementId(int)}
   */
  public int getStatementId() {
    return statementId;
  }

  public void setMaxStmtId(int maxStmtId) {
    this.maxStmtId = maxStmtId;
  }

  public int getMaxStmtId() {
    return maxStmtId;
  }

  public Path getDestPath() {
    return destPath;
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
  }


  @Override
  public String getTmpStatsDir() {
    return statsTmpDir;
  }

  public void setStatsTmpDir(String statsCollectionTempDir) {
    this.statsTmpDir = statsCollectionTempDir;
  }

  public void setMmWriteId(Long mmWriteId) {
    this.mmWriteId = mmWriteId;
  }

  public void setIsMerge(boolean b) {
    this.isMerge = b;
  }

  public boolean isMerge() {
    return isMerge;
  }

  public boolean isMmCtas() {
    return isMmCtas;
  }

  @Explain(displayName = "bucketingVersion", explainLevels = { Level.EXTENDED })
  public int getBucketingVersionForExplain() {
    return getBucketingVersion();
  }
  /**
   * Whether this is CREATE TABLE SELECT or CREATE MATERIALIZED VIEW statement
   * Set by semantic analyzer this is required because CTAS/CM requires some special logic
   * in mvFileToFinalPath
   */
  public boolean isCTASorCM() {
    return isCTASorCM;
  }

  public class FileSinkOperatorExplainVectorization extends OperatorExplainVectorization {

    public FileSinkOperatorExplainVectorization(VectorFileSinkDesc vectorFileSinkDesc) {
      // Native vectorization not supported.
      super(vectorFileSinkDesc, false);
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "File Sink Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public FileSinkOperatorExplainVectorization getFileSinkVectorization() {
    VectorFileSinkDesc vectorFileSinkDesc = (VectorFileSinkDesc) getVectorDesc();
    if (vectorFileSinkDesc == null) {
      return null;
    }
    return new FileSinkOperatorExplainVectorization(vectorFileSinkDesc);
  }

  public void setInsertOverwrite(boolean isInsertOverwrite) {
    this.isInsertOverwrite = isInsertOverwrite;
  }

  public boolean getInsertOverwrite() {
    return isInsertOverwrite;
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      FileSinkDesc otherDesc = (FileSinkDesc) other;
      return Objects.equals(getDirName(), otherDesc.getDirName()) &&
          Objects.equals(getTableInfo(), otherDesc.getTableInfo()) &&
          getCompressed() == otherDesc.getCompressed() &&
          getDestTableId() == otherDesc.getDestTableId() &&
          isMultiFileSpray() == otherDesc.isMultiFileSpray() &&
          getTotalFiles() == otherDesc.getTotalFiles() &&
          getNumFiles() == otherDesc.getNumFiles() &&
          Objects.equals(getStaticSpec(), otherDesc.getStaticSpec()) &&
          isGatherStats() == otherDesc.isGatherStats() &&
          Objects.equals(getStatsAggPrefix(), otherDesc.getStatsAggPrefix());
    }
    return false;
  }

  public String getMoveTaskId() {
    return moveTaskId;
  }

  public void setMoveTaskId(String moveTaskId) {
    this.moveTaskId = moveTaskId;
  }

  public Map<String, List<Path>> getDynPartitionValues() {
    return dynPartitionValues;
  }

  public void setDynPartitionValues(Map<String, List<Path>> dynPartitionValues) {
    this.dynPartitionValues = dynPartitionValues;
  }

}
