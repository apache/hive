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

@Explain(displayName = "File Output Operator")
public class FileSinkDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private String dirName;
  private TableDesc tableInfo;
  private boolean compressed;
  private int destTableId;
  private String compressCodec;
  private String compressType;

  public FileSinkDesc() {
  }

  public FileSinkDesc(final String dirName, final TableDesc tableInfo,
      final boolean compressed, int destTableId) {

    this.dirName = dirName;
    this.tableInfo = tableInfo;
    this.compressed = compressed;
    this.destTableId = destTableId;
  }

  public FileSinkDesc(final String dirName, final TableDesc tableInfo,
      final boolean compressed) {

    this.dirName = dirName;
    this.tableInfo = tableInfo;
    this.compressed = compressed;
    destTableId = 0;
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
}
