/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.worker;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcRecordUpdater;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/** Base {@link Mutator} implementation. Creates a suitable {@link RecordUpdater} and delegates mutation events. */
public class MutatorImpl implements Mutator {

  private final long transactionId;
  private final Path partitionPath;
  private final int bucketId;
  private final Configuration configuration;
  private final int recordIdColumn;
  private final ObjectInspector objectInspector;
  private RecordUpdater updater;

  public MutatorImpl(Configuration configuration, int recordIdColumn, ObjectInspector objectInspector,
      AcidOutputFormat<?, ?> outputFormat, long transactionId, Path partitionPath, int bucketId) throws IOException {
    this.configuration = configuration;
    this.recordIdColumn = recordIdColumn;
    this.objectInspector = objectInspector;
    this.transactionId = transactionId;
    this.partitionPath = partitionPath;
    this.bucketId = bucketId;

    updater = createRecordUpdater(outputFormat);
  }

  @Override
  public void insert(Object record) throws IOException {
    updater.insert(transactionId, record);
  }

  @Override
  public void update(Object record) throws IOException {
    updater.update(transactionId, record);
  }

  @Override
  public void delete(Object record) throws IOException {
    updater.delete(transactionId, record);
  }

  /**
   * This implementation does intentionally nothing at this time. We only use a single transaction and
   * {@link OrcRecordUpdater#flush()} will purposefully throw and exception in this instance. We keep this here in the
   * event that we support multiple transactions and to make it clear that the omission of an invocation of
   * {@link OrcRecordUpdater#flush()} was not a mistake.
   */
  @Override
  public void flush() throws IOException {
    // Intentionally do nothing
  }

  @Override
  public void close() throws IOException {
    updater.close(false);
    updater = null;
  }

  @Override
  public String toString() {
    return "ObjectInspectorMutator [transactionId=" + transactionId + ", partitionPath=" + partitionPath
        + ", bucketId=" + bucketId + "]";
  }

  protected RecordUpdater createRecordUpdater(AcidOutputFormat<?, ?> outputFormat) throws IOException {
    return outputFormat.getRecordUpdater(
        partitionPath,
        new AcidOutputFormat.Options(configuration)
            .inspector(objectInspector)
            .bucket(bucketId)
            .minimumTransactionId(transactionId)
            .maximumTransactionId(transactionId)
            .recordIdColumn(recordIdColumn)
            .finalDestination(partitionPath)
            .statementId(-1));
  }

}
