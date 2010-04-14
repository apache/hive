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

import java.io.Serializable;
import java.util.ArrayList;

/**
 * FileSinkDesc.
 *
 */
@Explain(displayName = "File Output Operator")
public class FileSinkDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private String dirName;
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

  @Explain(displayName = "directory", normalExplain = false)
  public String getDirName() {
    return dirName;
  }

  public void setDirName(final String dirName) {
    this.dirName = dirName;
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
}
