/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.vector;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class HiveBatchContext {

  private final VectorizedRowBatch batch;
  /**
   * File row position of the first row in this batch. Long.MIN_VALUE if unknown.
   */
  private final long fileRowOffset;

  public HiveBatchContext(VectorizedRowBatch batch, long fileRowOffset) {
    this.batch = batch;
    this.fileRowOffset = fileRowOffset;
  }

  public VectorizedRowBatch getBatch() {
    return batch;
  }

  public long getFileRowOffset() {
    return fileRowOffset;
  }
}
