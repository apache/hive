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
package org.apache.hadoop.hive.llap.io.decode;

import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;

public class OrcEncodedDataConsumer extends EncodedDataConsumer<OrcBatchKey> {
  private int previousStripeIndex;
  private RecordReaderImpl.TreeReader[] columnReaders;

  public OrcEncodedDataConsumer(
      ColumnVectorProducer<OrcBatchKey> cvp, Consumer<ColumnVectorBatch> consumer, int colCount) {
    super(cvp, consumer, colCount);
    this.previousStripeIndex = -1;
  }

  public int getPreviousStripeIndex() {
    return previousStripeIndex;
  }

  public void setPreviousStripeIndex(int previousStripeIndex) {
    this.previousStripeIndex = previousStripeIndex;
  }

  public void setColumnReaders(RecordReaderImpl.TreeReader[] columnReaders) {
    this.columnReaders = columnReaders;
  }

  public RecordReaderImpl.TreeReader[] getColumnReaders() {
    return columnReaders;
  }
}
