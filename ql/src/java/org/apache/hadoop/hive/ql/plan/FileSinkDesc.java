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

/**
 * FileSinkDesc.
 *
 */
@Explain(displayName = "File Output Operator")
public class FileSinkDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  private String dirName;
  // normally statsKeyPref will be the same as dirName, but the latter
  // could be changed in local execution optimization
  private String statsKeyPref;
  private TableDesc tableInfo;
  private boolean compressed;
  private int destTableId;
  private String compressCodec;
  private String compressType;
  private boolean multiFileSpray;
  private int     totalFiles;
  private ArrayList<ExprNodeDesc> partitionCols;
  private int     numFiles;
  private DynamicPartitionCtx dpCtx;
  private String staticSpec; // static partition spec ends with a '/'
  private boolean gatherStats;

  // This file descriptor is linked to other file descriptors.
  // One use case is that, a union->select (star)->file sink, is broken down.
  // For eg: consider a query like:
  // select * from (subq1 union all subq2)x;
  // where subq1 or subq2 involves a map-reduce job.
  // It is broken into two independent queries involving subq1 and subq2 directly, and
  // the sub-queries write to sub-directories of a common directory. So, the file sink
  // descriptors for subq1 and subq2 are linked.
  private boolean linkedFileSink = false;
  private String parentDir;
  transient private List<FileSinkDesc> linkedFileSinkDesc;

  private boolean statsReliable;

  public FileSinkDesc() {
  }

  public FileSinkDesc(final String dirName, final TableDesc tableInfo,
      final boolean compressed, final int destTableId, final boolean multiFileSpray,
      final int numFiles, final int totalFiles, final ArrayList<ExprNodeDesc> partitionCols,
      final DynamicPartitionCtx dpCtx) {

    this.dirName = dirName;
    this.tableInfo = tableInfo;
    this.compressed = compressed;
    this.destTableId = destTableId;
    this.multiFileSpray = multiFileSpray;
    this.numFiles = numFiles;
    this.totalFiles = totalFiles;
    this.partitionCols = partitionCols;
    this.dpCtx = dpCtx;
  }

  public FileSinkDesc(final String dirName, final TableDesc tableInfo,
      final boolean compressed) {

    this.dirName = dirName;
    this.tableInfo = tableInfo;
    this.compressed = compressed;
    destTableId = 0;
    this.multiFileSpray = false;
    this.numFiles = 1;
    this.totalFiles = 1;
    this.partitionCols = null;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    FileSinkDesc ret = new FileSinkDesc(dirName, tableInfo, compressed,
        destTableId, multiFileSpray, numFiles, totalFiles,
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
    return (Object) ret;
  }

  @Explain(displayName = "directory", normalExplain = false)
  public String getDirName() {
    return dirName;
  }

  public void setDirName(final String dirName) {
    this.dirName = dirName;
  }

  public String getFinalDirName() {
    return linkedFileSink ? parentDir : dirName;
  }

  @Explain(displayName = "table")
  public TableDesc getTableInfo() {
    return tableInfo;
  }

  public void setTableInfo(final TableDesc tableInfo) {
    this.tableInfo = tableInfo;
  }

  @Explain(displayName = "compressed")
  public boolean getCompressed() {
    return compressed;
  }

  public void setCompressed(boolean compressed) {
    this.compressed = compressed;
  }

  @Explain(displayName = "GlobalTableId")
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
  @Explain(displayName = "MultiFileSpray", normalExplain = false)
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
   * @return the totalFiles
   */
  @Explain(displayName = "TotalFiles", normalExplain = false)
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
  @Explain(displayName = "NumFilesPerFileSink", normalExplain = false)
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

  @Explain(displayName = "Static Partition Specification", normalExplain = false)
  public String getStaticSpec() {
    return staticSpec;
  }

  public void setGatherStats(boolean gatherStats) {
    this.gatherStats = gatherStats;
  }

  @Explain(displayName = "GatherStats", normalExplain = false)
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
  @Explain(displayName = "Stats Publishing Key Prefix", normalExplain = false)
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

  public String getParentDir() {
    return parentDir;
  }

  public void setParentDir(String parentDir) {
    this.parentDir = parentDir;
  }

  public boolean isStatsReliable() {
    return statsReliable;
  }

  public void setStatsReliable(boolean statsReliable) {
    this.statsReliable = statsReliable;
  }

  public List<FileSinkDesc> getLinkedFileSinkDesc() {
    return linkedFileSinkDesc;
  }

  public void setLinkedFileSinkDesc(List<FileSinkDesc> linkedFileSinkDesc) {
    this.linkedFileSinkDesc = linkedFileSinkDesc;
  }
}
