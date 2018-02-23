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
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcRecordUpdater;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/** Base {@link Mutator} implementation. Creates a suitable {@link RecordUpdater} and delegates mutation events. */
public class MutatorImpl implements Mutator {

  private final long writeId;
  private final Path partitionPath;
  private final int bucketProperty;
  private final Configuration configuration;
  private final int recordIdColumn;
  private final ObjectInspector objectInspector;
  private RecordUpdater updater;

  /**
   * @param bucketProperty - from existing {@link RecordIdentifier#getBucketProperty()}
   * @throws IOException
   */
  public MutatorImpl(Configuration configuration, int recordIdColumn, ObjectInspector objectInspector,
      AcidOutputFormat<?, ?> outputFormat, long writeId, Path partitionPath, int bucketProperty) throws IOException {
    this.configuration = configuration;
    this.recordIdColumn = recordIdColumn;
    this.objectInspector = objectInspector;
    this.writeId = writeId;
    this.partitionPath = partitionPath;
    this.bucketProperty = bucketProperty;

    updater = createRecordUpdater(outputFormat);
  }

  @Override
  public void insert(Object record) throws IOException {
    updater.insert(writeId, record);
  }

  @Override
  public void update(Object record) throws IOException {
    updater.update(writeId, record);
  }

  @Override
  public void delete(Object record) throws IOException {
    updater.delete(writeId, record);
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
    return "ObjectInspectorMutator [writeId=" + writeId + ", partitionPath=" + partitionPath
        + ", bucketId=" + bucketProperty + "]";
  }

  protected RecordUpdater createRecordUpdater(AcidOutputFormat<?, ?> outputFormat) throws IOException {
    int bucketId = BucketCodec
      .determineVersion(bucketProperty).decodeWriterId(bucketProperty); 
    return outputFormat.getRecordUpdater(
        partitionPath,
        new AcidOutputFormat.Options(configuration)
            .inspector(objectInspector)
            .bucket(bucketId)
            .minimumWriteId(writeId)
            .maximumWriteId(writeId)
            .recordIdColumn(recordIdColumn)
            .finalDestination(partitionPath)
            .statementId(-1));
  }

}
