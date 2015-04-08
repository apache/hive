/**
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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * FileSinkDesc.
 *
 */
@Explain(displayName = "File Output Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class FileSinkDesc extends AbstractOperatorDesc {
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
  // Whether the files output by this FileSink can be merged, e.g. if they are to be put into a
  // bucketed or sorted table/partition they cannot be merged.
  private boolean canBeMerged;
  private int     totalFiles;
  private ArrayList<ExprNodeDesc> partitionCols;
  private int     numFiles;
  private DynamicPartitionCtx dpCtx;
  private String staticSpec; // static partition spec ends with a '/'
  private boolean gatherStats;

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
  private Path parentDir;
  transient private List<FileSinkDesc> linkedFileSinkDesc;

  private boolean statsReliable;
  private ListBucketingCtx lbCtx;
  private int maxStatsKeyPrefixLength = -1;

  private boolean statsCollectRawDataSize;

  // Record what type of write this is.  Default is non-ACID (ie old style).
  private AcidUtils.Operation writeType = AcidUtils.Operation.NOT_ACID;
  private long txnId = 0;  // transaction id for this operation

  private transient Table table;

  public FileSinkDesc() {
  }

  public FileSinkDesc(final Path dirName, final TableDesc tableInfo,
      final boolean compressed, final int destTableId, final boolean multiFileSpray,
      final boolean canBeMerged, final int numFiles, final int totalFiles,
      final ArrayList<ExprNodeDesc> partitionCols, final DynamicPartitionCtx dpCtx) {

    this.dirName = dirName;
    this.tableInfo = tableInfo;
    this.compressed = compressed;
    this.destTableId = destTableId;
    this.multiFileSpray = multiFileSpray;
    this.canBeMerged = canBeMerged;
    this.numFiles = numFiles;
    this.totalFiles = totalFiles;
    this.partitionCols = partitionCols;
    this.dpCtx = dpCtx;
    this.dpSortState = DPSortState.NONE;
  }

  public FileSinkDesc(final Path dirName, final TableDesc tableInfo,
      final boolean compressed) {

    this.dirName = dirName;
    this.tableInfo = tableInfo;
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
    FileSinkDesc ret = new FileSinkDesc(dirName, tableInfo, compressed,
        destTableId, multiFileSpray, canBeMerged, numFiles, totalFiles,
        partitionCols, dpCtx);
    ret.setCompressCodec(compressCodec);
    ret.setCompressType(compressType);
    ret.setGatherStats(gatherStats);
    ret.setStaticSpec(staticSpec);
    ret.setStatsAggPrefix(statsKeyPref);
    ret.setLinkedFileSink(linkedFileSink);
    ret.setParentDir(parentDir);
    ret.setLinkedFileSinkDesc(linkedFileSinkDesc);
    ret.setStatsReliable(statsReliable);
    ret.setMaxStatsKeyPrefixLength(maxStatsKeyPrefixLength);
    ret.setStatsCollectRawDataSize(statsCollectRawDataSize);
    ret.setDpSortState(dpSortState);
    ret.setWriteType(writeType);
    ret.setTransactionId(txnId);
    return (Object) ret;
  }

  @Explain(displayName = "directory", explainLevels = { Level.EXTENDED })
  public Path getDirName() {
    return dirName;
  }

  public void setDirName(final Path dirName) {
    this.dirName = dirName;
  }

  public Path getFinalDirName() {
    return linkedFileSink ? parentDir : dirName;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public TableDesc getTableInfo() {
    return tableInfo;
  }

  public void setTableInfo(final TableDesc tableInfo) {
    this.tableInfo = tableInfo;
  }

  @Explain(displayName = "compressed", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean getCompressed() {
    return compressed;
  }

  public void setCompressed(boolean compressed) {
    this.compressed = compressed;
  }

  @Explain(displayName = "GlobalTableId", explainLevels = { Level.EXTENDED })
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

  /**
   * @param totalFiles the totalFiles to set
   */
  public void setTemporary(boolean temporary) {
    this.temporary = temporary;
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
  public ArrayList<ExprNodeDesc> getPartitionCols() {
    return partitionCols;
  }

  /**
   * @param partitionCols the partitionCols to set
   */
  public void setPartitionCols(ArrayList<ExprNodeDesc> partitionCols) {
    this.partitionCols = partitionCols;
  }

  /**
   * @return the numFiles
   */
  @Explain(displayName = "NumFilesPerFileSink", explainLevels = { Level.EXTENDED })
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
  public String getStaticSpec() {
    return staticSpec;
  }

  public void setGatherStats(boolean gatherStats) {
    this.gatherStats = gatherStats;
  }

  @Explain(displayName = "GatherStats", explainLevels = { Level.EXTENDED })
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
  @Explain(displayName = "Stats Publishing Key Prefix", explainLevels = { Level.EXTENDED })
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
    return parentDir;
  }

  public void setParentDir(Path parentDir) {
    this.parentDir = parentDir;
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

  public int getMaxStatsKeyPrefixLength() {
    return maxStatsKeyPrefixLength;
  }

  public void setMaxStatsKeyPrefixLength(int maxStatsKeyPrefixLength) {
    this.maxStatsKeyPrefixLength = maxStatsKeyPrefixLength;
  }

  public boolean isStatsCollectRawDataSize() {
    return statsCollectRawDataSize;
  }

  public void setStatsCollectRawDataSize(boolean statsCollectRawDataSize) {
    this.statsCollectRawDataSize = statsCollectRawDataSize;
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

  public void setDpSortState(DPSortState dpSortState) {
    this.dpSortState = dpSortState;
  }

  public void setWriteType(AcidUtils.Operation type) {
    writeType = type;
  }

  public AcidUtils.Operation getWriteType() {
    return writeType;
  }

  public void setTransactionId(long id) {
    txnId = id;
  }

  public long getTransactionId() {
    return txnId;
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
  }
}
