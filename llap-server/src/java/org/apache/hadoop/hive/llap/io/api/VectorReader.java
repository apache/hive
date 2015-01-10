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

import java.util.List;
import java.io.IOException;

import org.apache.hadoop.hive.llap.io.api.EncodedColumn.ColumnBuffer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

public interface VectorReader {
  /**
   * Unlike VRB, doesn't have some fields, and doesn't have all columns
   * (non-selected, partition cols, cols for downstream ops, etc.)
   */
  public static class ColumnVectorBatch {
    public ColumnVector[] cols;
    public int size;
    public List<ColumnBuffer> lockedBuffers;
  }
  public ColumnVectorBatch next() throws InterruptedException, IOException;
  public void close() throws IOException;
}
