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

@explain(displayName = "File Output Operator")
public class fileSinkDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private String dirName;
  private tableDesc tableInfo;
  private boolean compressed;
  private int destTableId;
  private String compressCodec;
  private String compressType;

  public fileSinkDesc() {
  }

  public fileSinkDesc(final String dirName, final tableDesc tableInfo,
      final boolean compressed, int destTableId) {

    this.dirName = dirName;
    this.tableInfo = tableInfo;
    this.compressed = compressed;
    this.destTableId = destTableId;
  }

  public fileSinkDesc(final String dirName, final tableDesc tableInfo,
      final boolean compressed) {

    this.dirName = dirName;
    this.tableInfo = tableInfo;
    this.compressed = compressed;
    destTableId = 0;
  }

  @explain(displayName = "directory", normalExplain = false)
  public String getDirName() {
    return dirName;
  }

  public void setDirName(final String dirName) {
    this.dirName = dirName;
  }

  @explain(displayName = "table")
  public tableDesc getTableInfo() {
    return tableInfo;
  }

  public void setTableInfo(final tableDesc tableInfo) {
    this.tableInfo = tableInfo;
  }

  @explain(displayName = "compressed")
  public boolean getCompressed() {
    return compressed;
  }

  public void setCompressed(boolean compressed) {
    this.compressed = compressed;
  }

  @explain(displayName = "GlobalTableId")
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
