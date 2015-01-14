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

package org.apache.hadoop.hive.llap.io.api;

import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;

public class EncodedColumn<BatchKey> {
  // TODO: temporary class. Will be filled in when reading (ORC) is implemented. Need to balance
  //       generality, and ability to not copy data from underlying low-level cached buffers.
  public static class ColumnBuffer {
    // TODO: given how ORC will allocate, it might make sense to share array between all
    //       returned encodedColumn-s, and store index and length in the array.
    public LlapMemoryBuffer[] cacheBuffers;
    public int firstOffset, lastLength;
  }
  public EncodedColumn(BatchKey batchKey, int columnIndex, ColumnBuffer columnData) {
    this.batchKey = batchKey;
    this.columnIndex = columnIndex;
    this.columnData = columnData;
  }

  public BatchKey batchKey;
  public int columnIndex;
  public ColumnBuffer columnData;
}